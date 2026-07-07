"""Domain data type variants used to describe table schemas."""

from dataclasses import dataclass

_MAX_DECIMAL_PRECISION = 38  # hard limit of Delta/Spark DecimalType


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
        precision: Total number of digits (1–38, Delta/Spark limit).
        scale: Digits to the right of the decimal point.

    """

    precision: int
    scale: int = 0

    def __post_init__(self) -> None:
        """Validate that 0 < precision <= 38 and 0 <= scale <= precision."""
        if self.precision <= 0 or not (0 <= self.scale <= self.precision):
            raise ValueError("invalid decimal(precision, scale)")
        if self.precision > _MAX_DECIMAL_PRECISION:
            raise ValueError(
                f"decimal precision must be at most {_MAX_DECIMAL_PRECISION}"
                f" (Delta/Spark limit); got {self.precision}"
            )


@dataclass(frozen=True, slots=True)
class Byte(DataType):
    """8-bit signed integer type (TINYINT)."""


@dataclass(frozen=True, slots=True)
class Short(DataType):
    """16-bit signed integer type (SMALLINT)."""


@dataclass(frozen=True, slots=True)
class Binary(DataType):
    """Sequence-of-bytes type."""


@dataclass(frozen=True, slots=True)
class TimestampNtz(DataType):
    """Timestamp with date and time, no timezone."""


@dataclass(frozen=True, slots=True)
class Variant(DataType):
    """Semi-structured value type (Databricks VARIANT)."""


@dataclass(frozen=True, slots=True)
class Array(DataType):
    """Array of homogeneous ``element`` values."""

    element: DataType


@dataclass(frozen=True, slots=True)
class Map(DataType):
    """Dictionary of ``key`` to ``value`` elements."""

    key: DataType
    value: DataType
