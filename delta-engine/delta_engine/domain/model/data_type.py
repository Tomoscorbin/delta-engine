from __future__ import annotations

from dataclasses import dataclass
from typing import Union

# ---- Scalar variants ---------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Int32: pass

@dataclass(frozen=True, slots=True)
class Int64: pass

@dataclass(frozen=True, slots=True)
class Float32: pass

@dataclass(frozen=True, slots=True)
class Float64: pass

@dataclass(frozen=True, slots=True)
class Boolean: pass

@dataclass(frozen=True, slots=True)
class String: pass

@dataclass(frozen=True, slots=True)
class Date: pass

@dataclass(frozen=True, slots=True)
class Timestamp: pass


# ---- Parameterized variants --------------------------------------------------

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
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    String,
    Date,
    Timestamp,
    Decimal,
    Array,
    Map
]
