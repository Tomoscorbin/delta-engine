"""
Load ``DeltaTable`` declarations from ``module[:attribute]`` specs.

A bare module spec collects every ``DeltaTable`` bound at the module's top
level (aggregator re-imports included), in definition order, deduplicated by
object identity. ``module:attribute`` targets one binding, which must be a
``DeltaTable`` or an iterable of them. The working directory is prepended to
``sys.path`` so declarations load from a repo checkout without installation.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
import importlib
import os
import sys
from types import ModuleType

from delta_engine.cli.errors import ConfigError, DeclarationImportError
from delta_engine.schema import DeltaTable


@dataclass(frozen=True)
class _DeclarationSpec:
    module_name: str
    attribute: str | None


def load_declarations(spec_texts: Sequence[str]) -> tuple[DeltaTable, ...]:
    """
    Resolve every ``module[:attribute]`` spec into the declared tables.

    Raises:
        ConfigError: For anticipated problems — malformed spec, missing module
            or attribute, an attribute of the wrong type, or no tables found.
        DeclarationImportError: When a declarations module raises while being
            imported; the user's exception is carried on ``__cause__``.

    """
    _ensure_working_directory_on_path()
    tables: list[DeltaTable] = []
    seen_ids: set[int] = set()
    for spec_text in spec_texts:
        spec = _parse_spec(spec_text)
        module = _import_module(spec.module_name)
        for table in _collect(module, spec):
            if id(table) in seen_ids:
                continue
            seen_ids.add(id(table))
            tables.append(table)
    if not tables:
        joined = ", ".join(spec_texts)
        raise ConfigError(f"no DeltaTable declarations found in: {joined}")
    return tuple(tables)


def _ensure_working_directory_on_path() -> None:
    working_directory = os.getcwd()
    if working_directory not in sys.path:
        sys.path.insert(0, working_directory)


def _parse_spec(text: str) -> _DeclarationSpec:
    parts = text.split(":")
    if len(parts) == 1:
        module_name, attribute = parts[0], None
    elif len(parts) == 2:
        module_name, attribute = parts[0], parts[1]
    else:
        raise ConfigError(f"malformed spec '{text}': expected module or module:attribute")
    if not module_name or (attribute is not None and not attribute):
        raise ConfigError(f"malformed spec '{text}': expected module or module:attribute")
    return _DeclarationSpec(module_name=module_name, attribute=attribute)


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
        raise DeclarationImportError(module_name) from error
    except Exception as error:
        raise DeclarationImportError(module_name) from error


def _collect(module: ModuleType, spec: _DeclarationSpec) -> tuple[DeltaTable, ...]:
    if spec.attribute is None:
        return tuple(value for value in vars(module).values() if isinstance(value, DeltaTable))
    try:
        value = getattr(module, spec.attribute)
    except AttributeError:
        raise ConfigError(
            f"module '{spec.module_name}' has no attribute '{spec.attribute}'"
        ) from None
    return _tables_from_attribute(value, spec)


def _tables_from_attribute(value: object, spec: _DeclarationSpec) -> tuple[DeltaTable, ...]:
    if isinstance(value, DeltaTable):
        return (value,)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        items = tuple(value)
        for item in items:
            if not isinstance(item, DeltaTable):
                raise ConfigError(
                    f"'{spec.module_name}:{spec.attribute}' contains "
                    f"{type(item).__name__}; expected only DeltaTable declarations"
                )
        return items
    raise ConfigError(
        f"'{spec.module_name}:{spec.attribute}' is {type(value).__name__}; "
        "expected a DeltaTable or an iterable of them"
    )
