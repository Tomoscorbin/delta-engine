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


def domain_type_from_spark(spark_type: str | SparkType) -> DataType:
    """Map a Spark SQL type (instance or DDL string) to a domain type."""
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
            return Array(domain_type_from_spark(spark_type.elementType))
        case MapType():
            return Map(
                domain_type_from_spark(spark_type.keyType),
                domain_type_from_spark(spark_type.valueType))
        case _:
            raise TypeError(f"Unsupported Spark type: {spark_type!r}")
