"""Domain data type variants used to describe table schemas."""

from collections.abc import Sequence
from dataclasses import dataclass

from delta_engine.domain.model.identifier import Identifier, identifier_key

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
        precision: Total number of digits (1-38, Delta/Spark limit).
        scale: Digits to the right of the decimal point.

    """

    precision: int
    scale: int = 0

    def __post_init__(self) -> None:
        """Validate that 0 < precision <= 38 and 0 <= scale <= precision."""
        # The cap is checked first so an over-limit precision always gets the
        # message naming the limit, even when the scale is also out of range.
        if self.precision > _MAX_DECIMAL_PRECISION:
            raise ValueError(
                f"decimal precision must be at most {_MAX_DECIMAL_PRECISION}"
                f" (Delta/Spark limit); got {self.precision}"
            )
        if self.precision <= 0 or not (0 <= self.scale <= self.precision):
            raise ValueError("invalid decimal(precision, scale)")


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
class StructField:
    """
    One named field inside a :class:`Struct`.

    Field nullability and comments are deliberately not modeled: catalog DDL
    strings do not round-trip them reliably, so both desired and observed
    structs normalize to name + type. Declared fields are created nullable.
    """

    name: str
    data_type: DataType

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError(f"Struct field name must not be blank: {self.name!r}")
        object.__setattr__(self, "name", Identifier(self.name))


@dataclass(frozen=True, slots=True)
class Struct(DataType):
    """Struct of named fields; identity is the ordered (name, type) tuple."""

    fields: Sequence[StructField]

    def __post_init__(self) -> None:
        # Accept any sequence at the public boundary; store a tuple so
        # equality and hashing stay structural.
        object.__setattr__(self, "fields", tuple(self.fields))
        if not self.fields:
            raise ValueError("Struct requires at least one field")
        seen: set[str] = set()
        for field in self.fields:
            if field.name in seen:
                raise ValueError(f"Duplicate struct field name: {field.name}")
            seen.add(field.name)


@dataclass(frozen=True, slots=True)
class Array(DataType):
    """Array of homogeneous ``element`` values."""

    element: DataType


@dataclass(frozen=True, slots=True)
class Map(DataType):
    """Dictionary of ``key`` to ``value`` elements."""

    key: DataType
    value: DataType

    def __post_init__(self) -> None:
        # Databricks accepts any MAP key type except MAP itself.
        if isinstance(self.key, Map):
            raise ValueError("Map key type must not be a Map")


def canonical_data_type(data_type: DataType) -> DataType:
    """
    Return the semantic identity form of ``data_type``.

    Struct field names are identifier-keyed so two types differing only in
    field-name case are the same managed type. Every other variant is its
    own identity. The original value's spelling is untouched: render and
    report from the original, compare through this.
    """
    match data_type:
        case Struct(fields=fields):
            return Struct(
                tuple(
                    StructField(identifier_key(field.name), canonical_data_type(field.data_type))
                    for field in fields
                )
            )
        case Array(element=element):
            return Array(canonical_data_type(element))
        case Map(key=key, value=value):
            return Map(canonical_data_type(key), canonical_data_type(value))
        case _:
            return data_type
