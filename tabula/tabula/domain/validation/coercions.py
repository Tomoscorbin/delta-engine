from __future__ import annotations
from typing import Iterable, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ..model.data_type import DataType

Param = Union[int, "DataType"]

def coerce_params(raw: Iterable[Param] | tuple[Param, ...]) -> tuple[Param, ...]:
    if isinstance(raw, tuple):
        return raw
    try:
        return tuple(raw)
    except TypeError as exc:
        raise TypeError("DataType.parameters must be an iterable of int or DataType") from exc

def validate_param_types(params: tuple[Param, ...]) -> None:
    from ..model.data_type import DataType  # local import to avoid cycles
    for value in params:
        if not isinstance(value, (int, DataType)):
            raise TypeError(
                f"Invalid parameter type: {type(value).__name__}; expected int or DataType"
            )
