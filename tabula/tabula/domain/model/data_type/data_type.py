"""Domain data type value object and helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from tabula.domain.model._identifiers import (
    normalize_identifier,
)  # ensure this does NOT import DataType


@dataclass(frozen=True, slots=True)
class DataType:
    """Engine-agnostic logical type, e.g., ``decimal(18,2)`` or ``array(int)``.

    Attributes:
        name: Type name such as ``int`` or ``decimal``.
        parameters: Optional type parameters.
    """

    name: str
    parameters: tuple[Param, ...] = ()

    def __post_init__(self) -> None:
        normalized_name = normalize_identifier(self.name)
        coerced = _coerce_params(self.parameters)
        _validate_by_type(normalized_name, coerced)

        object.__setattr__(self, "name", normalized_name)
        object.__setattr__(self, "parameters", coerced)


# ---- Type aliases (after the class so no quoted forward refs) -----------------

Param: type = int | DataType


# ---- Coercion & basic type validation (domain-specific, no imports) ----------


def _coerce_params(raw: tuple[Param, ...] | object) -> tuple[Param, ...]:
    """Normalize a parameter iterable.

    Args:
        raw: Iterable of parameters or a tuple of them.

    Returns:
        Tuple of parameters.

    Raises:
        TypeError: If ``raw`` is not iterable.
    """

    if isinstance(raw, tuple):
        return raw
    try:
        # Treat strings as non-iterables for our use-case; if needed, adjust here.
        return tuple(raw)  # type: ignore[arg-type]
    except TypeError as exc:
        raise TypeError("DataType.parameters must be an iterable of int or DataType") from exc


def _is_datatype_like(x: object) -> bool:
    """Return ``True`` if ``x`` has ``name`` and ``parameters`` attributes."""

    return hasattr(x, "name") and hasattr(x, "parameters")


# ---- Per-type rules via a tiny registry --------------------------------------

Validator = Callable[[tuple[Param, ...]], None]
_VALIDATORS: dict[str, Validator] = {}


def register_type(name: str) -> Callable[[Validator], Validator]:
    """Register a validator function for a type name."""

    def _decorator(fn: Validator) -> Validator:
        _VALIDATORS[name] = fn
        return fn

    return _decorator


def _validate_by_type(name: str, params: tuple[Param, ...]) -> None:
    """Run a registered validator for ``name`` if one exists."""

    validator = _VALIDATORS.get(name)
    if validator:
        validator(params)


@register_type("decimal")
def _validate_decimal(params: tuple[Param, ...]) -> None:
    if len(params) != 2 or not all(isinstance(p, int) for p in params):
        raise ValueError("decimal requires (precision:int, scale:int)")
    precision, scale = params  # type: ignore[assignment]
    if precision <= 0 or scale < 0 or scale > precision:
        raise ValueError("invalid decimal precision/scale")


@register_type("array")
def _validate_array(params: tuple[Param, ...]) -> None:
    if len(params) != 1:
        raise ValueError("array requires exactly one parameter (element type)")
    element = params[0]
    if not _is_datatype_like(element):
        raise TypeError("array parameter must be a DataType")


@register_type("map")
def _validate_map(params: tuple[Param, ...]) -> None:
    if len(params) != 2:
        raise ValueError("map requires exactly two parameters (key type, value type)")
    key_type, value_type = params
    if not (_is_datatype_like(key_type) and _is_datatype_like(value_type)):
        raise TypeError("map parameters must be DataType")
