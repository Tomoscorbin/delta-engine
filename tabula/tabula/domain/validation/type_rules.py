from __future__ import annotations
from typing import Callable, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ..model.data_type import DataType

Param = Union[int, "DataType"]
Validator = Callable[[tuple[Param, ...]], None]

_VALIDATORS: dict[str, Validator] = {}

def register_type(name: str) -> Callable[[Validator], Validator]:
    def _decorator(fn: Validator) -> Validator:
        _VALIDATORS[name] = fn
        return fn
    return _decorator

def validate_by_type(name: str, params: tuple[Param, ...]) -> None:
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
    if not hasattr(element, "name"):
        raise TypeError("array parameter must be a DataType")

@register_type("map")
def _validate_map(params: tuple[Param, ...]) -> None:
    if len(params) != 2:
        raise ValueError("map requires exactly two parameters (key type, value type)")
    key_type, value_type = params
    if not (hasattr(key_type, "name") and hasattr(value_type, "name")):
        raise TypeError("map parameters must be DataType")
