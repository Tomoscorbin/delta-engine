"""Domain data type variants used to describe table schemas."""

from __future__ import annotations

from dataclasses import dataclass

# TODO: this might be better inside each data type class
def _format_datatype(data_type: DataType) -> str:
    match data_type:
        case Integer():
            return "int"
        case Long():
            return "long"
        case Float():
            return "float"
        case Double():
            return "double"
        case Boolean():
            return "boolean"
        case String():
            return "string"
        case Date():
            return "date"
        case Timestamp():
            return "timestamp"
        case Decimal(precision=p, scale=s):
            return f"decimal({p},{s})"
        case Array(element=e):
            return f"array<{_format_datatype(e)}>"
        case Map(key=k, value=v):
            return f"map<{_format_datatype(k)},{_format_datatype(v)}>"
        case _:
            raise TypeError(f"Unsupported DataType: {type(data_type).__name__}")


class DataType:
    """Base class for all data types."""

    def __str__(self) -> str:
        """Return a user-friendly string representation of the data type."""
        return _format_datatype(self)


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
