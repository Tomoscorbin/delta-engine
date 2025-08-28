from __future__ import annotations

from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True, slots=True)
class Integer: pass

@dataclass(frozen=True, slots=True)
class Long: pass

@dataclass(frozen=True, slots=True)
class Float: pass

@dataclass(frozen=True, slots=True)
class Double: pass

@dataclass(frozen=True, slots=True)
class Boolean: pass

@dataclass(frozen=True, slots=True)
class String: pass

@dataclass(frozen=True, slots=True)
class Date: pass

@dataclass(frozen=True, slots=True)
class Timestamp: pass



@dataclass(frozen=True, slots=True)
class Decimal:
    precision: int
    scale: int = 0

    def __post_init__(self) -> None:
        if self.precision <= 0 or not (0 <= self.scale <= self.precision):
            raise ValueError("invalid decimal(precision, scale)")

@dataclass(frozen=True, slots=True)
class Array:
    element: DataType

@dataclass(frozen=True, slots=True)
class Map:
    key: DataType
    value: DataType


# Public union for type annotations
DataType = Union[
    Integer,
    Long,
    Float,
    Double,
    Boolean,
    String,
    Date,
    Timestamp,
    Decimal,
    Array,
    Map
]
