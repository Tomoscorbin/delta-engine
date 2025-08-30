"""Domain data type variants used to describe table schemas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True, slots=True)
class Integer:
    """32-bit signed integer type."""


@dataclass(frozen=True, slots=True)
class Long:
    """64-bit signed integer type."""


@dataclass(frozen=True, slots=True)
class Float:
    """32-bit floating point type."""


@dataclass(frozen=True, slots=True)
class Double:
    """64-bit floating point type."""


@dataclass(frozen=True, slots=True)
class Boolean:
    """Boolean truth value type."""


@dataclass(frozen=True, slots=True)
class String:
    """Unicode string type."""


@dataclass(frozen=True, slots=True)
class Date:
    """Calendar date without time or timezone."""


@dataclass(frozen=True, slots=True)
class Timestamp:
    """Timestamp with date and time (timezone handling is engine-specific)."""


@dataclass(frozen=True, slots=True)
class Decimal:
    """
    Fixed-precision decimal type.

    Attributes:
        precision: Total number of digits.
        scale: Digits to the right of the decimal point.

    """

    precision: int
    scale: int = 0

    def __post_init__(self) -> None:
        if self.precision <= 0 or not (0 <= self.scale <= self.precision):
            raise ValueError("invalid decimal(precision, scale)")


@dataclass(frozen=True, slots=True)
class Array:
    """Array of homogeneous ``element`` values."""

    element: DataType


@dataclass(frozen=True, slots=True)
class Map:
    """Dictionary of ``key`` to ``value`` elements."""

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
    Map,
]
