"""Load one ordered collection of declarations from ``MODULE:ATTRIBUTE``."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
import importlib
import os
import sys
from types import ModuleType
from typing import Self

from delta_engine.application.engine import lower_desired_tables
from delta_engine.cli.errors import ConfigError
from delta_engine.schema import DeltaTable

_MISSING = object()


@dataclass(frozen=True, slots=True)
class DeclarationRef:
    """The explicit module attribute containing a declaration collection."""

    module_name: str
    attribute: str

    @classmethod
    def parse(cls, text: str) -> Self:
        """Parse exactly one importable ``MODULE:ATTRIBUTE`` reference."""
        module_name, separator, attribute = text.partition(":")
        module_is_importable = bool(separator) and all(
            part.isidentifier() for part in module_name.split(".")
        )
        if not module_is_importable or not attribute.isidentifier():
            raise ConfigError(
                f"malformed declaration reference '{text}': expected MODULE:ATTRIBUTE, "
                "such as myproject.tables:all_tables (a module name, not a file path)"
            )
        return cls(module_name=module_name, attribute=attribute)

    def __str__(self) -> str:
        """Return the command-line representation."""
        return f"{self.module_name}:{self.attribute}"


def load_declarations(reference: DeclarationRef) -> tuple[DeltaTable, ...]:
    """
    Resolve ``reference`` into one non-empty ordered sequence of tables.

    A bare ``DeltaTable`` attribute loads as a one-table collection. Duplicate
    qualified names fail before authentication or catalog access begins. The
    working directory fronts ``sys.path`` only while the declarations load, so
    an uninstalled project module resolves; ``sys.path`` is restored
    afterwards, and nothing the load did to it outlives the call.

    Raises:
        ConfigError: If the target module or attribute is missing, or the
            attribute is neither a ``DeltaTable`` nor a non-empty ordered
            sequence of ``DeltaTable``.
        DuplicateTableDefinitionError: If the sequence defines one qualified
            table more than once.
        Exception: Any exception raised by imported user code is allowed to
            propagate with its original traceback.

    """
    with _front_working_directory_on_path():
        module = _import_module(reference.module_name)
        value = _attribute(module, reference)
        tables = _tables_from_attribute(value, reference)
    # The engine's own lowering step owns the duplicate-name rule; running
    # it here surfaces the same typed error before a connection is opened.
    lower_desired_tables(*tables)
    return tables


@contextmanager
def _front_working_directory_on_path() -> Iterator[None]:
    """Place the working directory first on ``sys.path`` within the block only."""
    # Fronting the working directory lets a stray local databricks.py shadow
    # the installed SDK while declarations load; the stray module then stays
    # cached in sys.modules past the restore, which is what keeps
    # connection._shadowing_module_file able to diagnose it. Change either
    # policy only together with the other.
    original = list(sys.path)
    working_directory = os.getcwd()
    sys.path[:] = [
        working_directory,
        *(entry for entry in original if entry != working_directory),
    ]
    try:
        yield
    finally:
        sys.path[:] = original


def _import_module(module_name: str) -> ModuleType:
    """Import the selected module while distinguishing its own missing dependencies."""
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as error:
        target_is_missing = error.name is not None and (
            error.name == module_name or module_name.startswith(error.name + ".")
        )
        if target_is_missing:
            raise ConfigError(
                f"module '{module_name}' not found; run from the project root "
                "or install the package that contains it"
            ) from error
        raise


def _attribute(module: ModuleType, reference: DeclarationRef) -> object:
    """Fetch the selected attribute; lazy module ``__getattr__`` hooks participate."""
    value = getattr(module, reference.attribute, _MISSING)
    if value is _MISSING:
        raise ConfigError(
            f"module '{reference.module_name}' has no attribute '{reference.attribute}'"
        )
    return value


def _tables_from_attribute(
    value: object,
    reference: DeclarationRef,
) -> tuple[DeltaTable, ...]:
    """Validate and freeze the selected ordered declaration collection."""
    reference_text = str(reference)
    if isinstance(value, DeltaTable):
        return (value,)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ConfigError(
            f"'{reference_text}' is {type(value).__name__}; "
            "expected a non-empty ordered sequence of DeltaTable declarations"
        )

    items = tuple(value)
    if not items:
        raise ConfigError(
            f"'{reference_text}' is empty; expected at least one DeltaTable declaration"
        )
    for index, item in enumerate(items):
        if not isinstance(item, DeltaTable):
            raise ConfigError(
                f"'{reference_text}' item {index} is {type(item).__name__}; "
                "expected only DeltaTable declarations"
            )
    return items
