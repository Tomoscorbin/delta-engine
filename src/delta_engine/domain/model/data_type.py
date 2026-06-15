"""Domain data type variants used to describe table schemas."""

from __future__ import annotations

from dataclasses import dataclass


class DataType:
    """Base class for all data types."""


@dataclass(frozen=True, slots=True)
class Integer(DataType):
    """32-bit signed integer type."""


@dataclass(frozen=True, slots=True)
class Long(DataType):
    """64-bit signed integer type."""


@dataclass(frozen=True, slots=True)
class Float(DataType):
    """32-bit floating point type."""


@dataclass(frozen=True, slots=True)
class Double(DataType):
    """64-bit floating point type."""


@dataclass(frozen=True, slots=True)
class Boolean(DataType):
    """Boolean truth value type."""


@dataclass(frozen=True, slots=True)
class String(DataType):
    """Unicode string type."""


@dataclass(frozen=True, slots=True)
class Date(DataType):
    """Calendar date without time or timezone."""


@dataclass(frozen=True, slots=True)
class Timestamp(DataType):
    """Timestamp with date and time (timezone handling is engine-specific)."""


@dataclass(frozen=True, slots=True)
class Decimal(DataType):
    """
    Fixed-precision decimal type.

    Attributes:
        precision: Total number of digits.
        scale: Digits to the right of the decimal point.

    """

    precision: int
    scale: int = 0

    def __post_init__(self) -> None:
        """Validate that precision > 0 and 0 <= scale <= precision."""
        if self.precision <= 0 or not (0 <= self.scale <= self.precision):
            raise ValueError("invalid decimal(precision, scale)")


@dataclass(frozen=True, slots=True)
class Array(DataType):
    """Array of homogeneous ``element`` values."""

    element: DataType


@dataclass(frozen=True, slots=True)
class Map(DataType):
    """Dictionary of ``key`` to ``value`` elements."""

    key: DataType
    value: DataType
