"""
Map between domain `DataType` and Spark SQL types.

Provides conversions to Spark SQL DDL strings and from
`pyspark.sql.types.DataType` instances or DDL strings back to domain types.

Uses ``match``/``case`` rather than ``functools.singledispatch`` (which the plan
compiler uses): ``DataType`` is a closed set and the mapping is a leaf lookup,
where structural patterns like ``case Decimal(precision, scale)`` and
``case Array(element)`` destructure fields inline. ``singledispatch`` fits the
compiler because the ``Action`` hierarchy is open to extension; it would only add
ceremony here.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType as SparkType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    TimestampType,
)

from delta_engine.domain.model import (
    Array,
    Boolean,
    DataType,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Map,
    String,
    Timestamp,
)


def sql_type_for_data_type(data_type: DataType) -> str:
    """Return a Spark SQL type string for a domain :class:`DataType`."""
    match data_type:
        case Integer():
            return "INT"
        case Long():
            return "BIGINT"
        case Float():
            return "FLOAT"
        case Double():
            return "DOUBLE"
        case Boolean():
            return "BOOLEAN"
        case String():
            return "STRING"
        case Date():
            return "DATE"
        case Timestamp():
            return "TIMESTAMP"
        case Decimal(precision, scale):
            return f"DECIMAL({precision},{scale})"
        case Array(element):
            return f"ARRAY<{sql_type_for_data_type(element)}>"
        case Map(key, value):
            return f"MAP<{sql_type_for_data_type(key)},{sql_type_for_data_type(value)}>"
        case _:
            cls = data_type.__class__.__name__
            raise TypeError(f"Unsupported DataType variant: {cls}")


def domain_type_from_spark(spark_type: str | SparkType) -> DataType | None:
    """
    Map a Spark SQL type (instance or DDL string) to a domain type.

    Returns ``None`` when the type has no domain mapping (e.g. ``BINARY``,
    ``STRUCT``, ``VARIANT``, ``TIMESTAMP_NTZ``). An unmappable element inside an
    ``ARRAY`` or ``MAP`` makes the whole type unmappable. An unmappable type is a
    routine, expected condition -- new Spark types appear over time -- so it is a
    ``None`` return, not an exception. Callers decide what to do with ``None``
    (the reader skips the column and logs a warning).
    """
    if isinstance(spark_type, str):
        spark_type = SparkType.fromDDL(spark_type)

    match spark_type:
        case IntegerType():
            return Integer()
        case LongType():
            return Long()
        case FloatType():
            return Float()
        case DoubleType():
            return Double()
        case BooleanType():
            return Boolean()
        case StringType():
            return String()
        case DateType():
            return Date()
        case TimestampType():
            return Timestamp()
        case DecimalType():
            return Decimal(spark_type.precision, spark_type.scale)
        case ArrayType():
            element = domain_type_from_spark(spark_type.elementType)
            return Array(element) if element is not None else None
        case MapType():
            key = domain_type_from_spark(spark_type.keyType)
            value = domain_type_from_spark(spark_type.valueType)
            if key is None or value is None:
                return None
            return Map(key, value)
        case _:
            return None
