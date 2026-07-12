"""Load ``DeltaTable`` declarations from explicit ``MODULE:ATTRIBUTE`` specs."""

from collections.abc import Sequence
from dataclasses import dataclass
import importlib
import os
import sys
from types import ModuleType

from delta_engine.cli.errors import ConfigError
from delta_engine.schema import DeltaTable


@dataclass(frozen=True)
class _DeclarationSpec:
    module_name: str
    attribute: str


def load_declarations(spec_texts: Sequence[str]) -> tuple[DeltaTable, ...]:
    """
    Resolve every ``MODULE:ATTRIBUTE`` spec into explicitly selected tables.

    The attribute may hold one :class:`DeltaTable` or an iterable of them.
    Duplicate qualified names are rejected with both source locations before
    the caller resolves authentication or opens a connection.

    Raises:
        ConfigError: For a malformed spec, missing target module or attribute,
            wrong or empty attribute value, or duplicate qualified table name.
        Exception: Any exception raised by imported user code is allowed to
            propagate with its original traceback.

    """
    if not spec_texts:
        raise ConfigError("at least one declaration spec is required")

    _ensure_working_directory_on_path()
    tables: list[DeltaTable] = []
    origins_by_name: dict[str, str] = {}
    for argument_index, spec_text in enumerate(spec_texts, start=1):
        spec = _parse_spec(spec_text)
        module = _import_module(spec.module_name)
        value = _attribute(module, spec)
        selected, is_iterable = _tables_from_attribute(value, spec_text)
        for item_index, table in enumerate(selected):
            origin = _origin(spec_text, argument_index, item_index, is_iterable=is_iterable)
            qualified_name = str(table.to_desired_table().qualified_name)
            previous_origin = origins_by_name.get(qualified_name)
            if previous_origin is not None:
                raise ConfigError(
                    f"duplicate table definition '{qualified_name}': "
                    f"selected by {previous_origin} and {origin}"
                )
            origins_by_name[qualified_name] = origin
            tables.append(table)
    return tuple(tables)


def _ensure_working_directory_on_path() -> None:
    """Place the working directory first, even when it already appears later."""
    working_directory = os.getcwd()
    sys.path[:] = [entry for entry in sys.path if entry != working_directory]
    sys.path.insert(0, working_directory)


def _parse_spec(text: str) -> _DeclarationSpec:
    parts = text.split(":")
    if len(parts) != 2 or not all(parts):
        raise ConfigError(f"malformed spec '{text}': expected MODULE:ATTRIBUTE")
    return _DeclarationSpec(module_name=parts[0], attribute=parts[1])


def _import_module(module_name: str) -> ModuleType:
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


def _attribute(module: ModuleType, spec: _DeclarationSpec) -> object:
    namespace = vars(module)
    if spec.attribute not in namespace:
        raise ConfigError(f"module '{spec.module_name}' has no attribute '{spec.attribute}'")
    return namespace[spec.attribute]


def _tables_from_attribute(value: object, spec_text: str) -> tuple[tuple[DeltaTable, ...], bool]:
    if isinstance(value, DeltaTable):
        return (value,), False
    # Sequences only: an unordered container (a set, say) would make item
    # indices in duplicate-definition messages depend on iteration order.
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        items = tuple(value)
        if not items:
            raise ConfigError(
                f"'{spec_text}' is empty; expected at least one DeltaTable declaration"
            )
        for index, item in enumerate(items):
            if not isinstance(item, DeltaTable):
                raise ConfigError(
                    f"'{spec_text}' item {index} is {type(item).__name__}; "
                    "expected only DeltaTable declarations"
                )
        return items, True
    raise ConfigError(
        f"'{spec_text}' is {type(value).__name__}; expected a DeltaTable "
        "or a sequence (list or tuple) of them"
    )


def _origin(spec_text: str, argument_index: int, item_index: int, *, is_iterable: bool) -> str:
    item = f"[{item_index}]" if is_iterable else ""
    return f"'{spec_text}{item}' (argument {argument_index})"
