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
    Float32,
    Float64,
    Int32,
    Int64,
    Map,
    String,
    Timestamp,
)


def sql_type_for_data_type(type: DataType) -> str:
    match type:
        case Int32():
            return "INT"
        case Int64():
            return "BIGINT"
        case Float32():
            return "FLOAT"
        case Float64():
            return "DOUBLE"
        case Boolean():
            return "BOOLEAN"
        case String():
            return "STRING"
        case Date():
            return "DATE"
        case Timestamp():
            return "TIMESTAMP"
        case Decimal(p, s):
            return f"DECIMAL({p},{s})"
        case Array(elem):
            return f"ARRAY<{sql_type_for_data_type(elem)}>"
        case Map(k, v):
            return f"MAP<{sql_type_for_data_type(k)},{sql_type_for_data_type(v)}>"
        case _:
            cls = type.__class__.__name__
            raise TypeError(f"Unsupported DataType variant: {cls}")


def domain_type_from_spark(type: SparkType) -> DataType:
    if isinstance(type, IntegerType):
        return Int32()
    if isinstance(type, LongType):
        return Int64()
    if isinstance(type, FloatType):
        return Float32()
    if isinstance(type, DoubleType):
        return Float64()
    if isinstance(type, BooleanType):
        return Boolean()
    if isinstance(type, StringType):
        return String()
    if isinstance(type, DateType):
        return Date()
    if isinstance(type, TimestampType):
        return Timestamp()
    if isinstance(type, DecimalType):
        return Decimal(type.precision, type.scale)
    if isinstance(type, ArrayType):
        return Array(domain_type_from_spark(type.elementType))
    if isinstance(type, MapType):
        return Map(
            domain_type_from_spark(type.keyType),
            domain_type_from_spark(type.valueType),
        )
    raise TypeError(f"Unsupported Spark type: {type!r}")
