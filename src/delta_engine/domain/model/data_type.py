"""Domain data type variants used to describe table schemas."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import ClassVar, Final, Self

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model.feature import TableFeature
from delta_engine.domain.model.identifier import Identifier

_MAX_DECIMAL_PRECISION = 38  # hard limit of Delta/Spark DecimalType


class DataType:
    """
    Base class of the closed set of data types; only this module defines variants.

    Attributes:
        required_feature: Delta table feature a table's protocol must support
            before a column of this type can exist, or ``None``.

    """

    required_feature: ClassVar[TableFeature | None] = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if cls.__module__ != __name__:
            raise TypeError(
                f"DataType is a closed vocabulary; cannot define variant {cls.__name__!r}"
            )

    def __new__(cls, *args: object, **kwargs: object) -> Self:
        """Construct a concrete variant; the abstract base itself is rejected."""
        if cls is DataType:
            raise TypeError("DataType is abstract; construct a concrete variant")
        return super().__new__(cls)

    def __str__(self) -> str:
        return type(self).__name__


def _require_data_type(value: object, *, subject: str) -> None:
    if not isinstance(value, DataType):
        raise TypeError(f"{subject} must be a DataType instance; got {value!r}")


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
        if type(self.precision) is not int or type(self.scale) is not int:
            raise TypeError(
                "precision and scale must be type int;"
                f" got precision: {type(self.precision)}, scale: {type(self.scale)}"
            )
        if not (1 <= self.precision <= _MAX_DECIMAL_PRECISION):
            raise ValueError(
                f"decimal precision must be between 1 and {_MAX_DECIMAL_PRECISION}"
                f" (Delta/Spark limit); got {self.precision}"
            )
        if not (0 <= self.scale <= self.precision):
            raise ValueError(
                f"decimal scale must be between 0 and precision ({self.precision});"
                f" got {self.scale}"
            )

    def __str__(self) -> str:
        return f"Decimal({self.precision},{self.scale})"


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

    required_feature: ClassVar[TableFeature] = TableFeature.TIMESTAMP_NTZ


@dataclass(frozen=True, slots=True)
class Variant(DataType):
    """Semi-structured value type (Databricks VARIANT)."""

    required_feature: ClassVar[TableFeature] = TableFeature.VARIANT


@dataclass(frozen=True, slots=True)
class StructField:
    """
    One named field inside a :class:`Struct`.

    Nullability is part of the field's identity and defaults to nullable,
    matching Databricks SQL. Nested field comments remain unmanaged.
    """

    name: str
    data_type: DataType
    nullable: bool = True

    def __post_init__(self) -> None:
        _require_data_type(self.data_type, subject="Struct field data type")
        if type(self.nullable) is not bool:
            raise TypeError(f"Struct field nullable must be a bool; got {self.nullable!r}")
        if not self.name.strip():
            raise ValueError(f"Struct field name must not be blank: {self.name!r}")
        object.__setattr__(self, "name", Identifier(self.name))

    def __str__(self) -> str:
        nullability = "" if self.nullable else " NOT NULL"
        return f"{self.name}: {self.data_type}{nullability}"


@dataclass(frozen=True, slots=True)
class Struct(DataType):
    """Struct of named fields; identity is their ordered name, type, and nullability."""

    fields: ListOrTuple[StructField]

    def __post_init__(self) -> None:
        # Accept any sequence at the public boundary; store a tuple so
        # equality and hashing stay structural.
        object.__setattr__(self, "fields", tuple(self.fields))
        if not self.fields:
            raise ValueError("Struct requires at least one field")
        seen: set[str] = set()
        for field in self.fields:
            if not isinstance(field, StructField):
                raise TypeError(f"Struct field must be a StructField instance; got {field!r}")
            if field.name in seen:
                raise ValueError(f"Duplicate struct field name: {field.name}")
            seen.add(field.name)

    def __str__(self) -> str:
        return f"Struct<{', '.join(str(field) for field in self.fields)}>"


@dataclass(frozen=True, slots=True)
class Array(DataType):
    """Array of homogeneous ``element`` values."""

    element: DataType

    def __post_init__(self) -> None:
        _require_data_type(self.element, subject="Array element")

    def __str__(self) -> str:
        return f"Array<{self.element}>"


@dataclass(frozen=True, slots=True)
class Map(DataType):
    """Dictionary of ``key`` to ``value`` elements."""

    key: DataType
    value: DataType

    def __post_init__(self) -> None:
        _require_data_type(self.key, subject="Map key")
        _require_data_type(self.value, subject="Map value")
        # Databricks accepts any MAP key type except MAP itself.
        if isinstance(self.key, Map):
            raise ValueError("Map key type must not be a Map")

    def __str__(self) -> str:
        return f"Map<{self.key}, {self.value}>"


def walk_data_type(data_type: DataType) -> Iterator[DataType]:
    """Yield ``data_type`` and every type nested inside it, depth-first."""
    yield data_type
    match data_type:
        case Array(element=element):
            yield from walk_data_type(element)
        case Map(key=key, value=value):
            yield from walk_data_type(key)
            yield from walk_data_type(value)
        case Struct(fields=fields):
            for field in fields:
                yield from walk_data_type(field.data_type)


# The widenings Delta can apply in place (observed -> desired), per the
# Databricks type-widening matrix. Decimal targets are handled separately —
# whether they fit depends on precision and scale, not the type alone.
# Composite types are deliberately absent: this engine models arrays, maps,
# and structs atomically, so they are never widened as a whole and any change
# to them stays blocked (Delta itself can widen nested fields).
_WIDENING_TARGETS: Final[Mapping[type[DataType], frozenset[type[DataType]]]] = MappingProxyType(
    {
        Byte: frozenset({Short, Integer, Long, Double}),
        Short: frozenset({Integer, Long, Double}),
        Integer: frozenset({Long, Double}),
        Float: frozenset({Double}),
        Date: frozenset({TimestampNtz}),
    }
)

# Widening an integer column to Decimal needs room for every value the source
# type can hold. Databricks specifies DECIMAL(10,0) as the minimum for
# Byte/Short/Integer and DECIMAL(20,0) for Long — i.e. this many integer
# digits (precision minus scale).
_DECIMAL_INTEGER_DIGITS_REQUIRED: Final[Mapping[type[DataType], int]] = MappingProxyType(
    {
        Byte: 10,
        Short: 10,
        Integer: 10,
        Long: 20,
    }
)


def can_widen_in_place(observed: DataType, desired: DataType) -> bool:
    """Return whether Delta type widening can change ``observed`` to ``desired`` in place."""
    if isinstance(desired, Decimal):
        return _can_widen_to_decimal_in_place(observed, desired)
    return type(desired) in _WIDENING_TARGETS.get(type(observed), frozenset())


def _can_widen_to_decimal_in_place(observed: DataType, desired: Decimal) -> bool:
    """Decimal widening keeps integer digits: scale may grow only if precision grows with it."""
    desired_integer_digits = desired.precision - desired.scale
    if isinstance(observed, Decimal):
        return (
            desired.scale >= observed.scale
            and desired_integer_digits >= observed.precision - observed.scale
        )
    required_integer_digits = _DECIMAL_INTEGER_DIGITS_REQUIRED.get(type(observed))
    if required_integer_digits is None:
        return False
    return desired_integer_digits >= required_integer_digits
