"""Convert backend-neutral desired schemas to native PySpark schemas."""

from pyspark.sql import types as spark_types

from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
    DataType,
    Date,
    Decimal,
    DesiredTable,
    Double,
    Float,
    Integer,
    Long,
    Map,
    Short,
    String,
    Struct,
    Timestamp,
    TimestampNtz,
    Variant,
)


def _to_spark_data_type(data_type: DataType) -> spark_types.DataType:
    """Return the native PySpark representation of one domain data type."""
    match data_type:
        case Integer():
            return spark_types.IntegerType()
        case Long():
            return spark_types.LongType()
        case Float():
            return spark_types.FloatType()
        case Double():
            return spark_types.DoubleType()
        case Boolean():
            return spark_types.BooleanType()
        case String():
            return spark_types.StringType()
        case Date():
            return spark_types.DateType()
        case Timestamp():
            return spark_types.TimestampType()
        case Decimal(precision, scale):
            return spark_types.DecimalType(precision, scale)
        case Array(element):
            # The declaration model does not express element nullability;
            # nullable matches Spark SQL's default ARRAY semantics.
            return spark_types.ArrayType(_to_spark_data_type(element), containsNull=True)
        case Map(key, value):
            # Spark map keys are always non-null. The declaration model does
            # not express value nullability, so use Spark SQL's nullable default.
            return spark_types.MapType(
                _to_spark_data_type(key),
                _to_spark_data_type(value),
                valueContainsNull=True,
            )
        case Byte():
            return spark_types.ByteType()
        case Short():
            return spark_types.ShortType()
        case Binary():
            return spark_types.BinaryType()
        case TimestampNtz():
            return spark_types.TimestampNTZType()
        case Variant():
            return spark_types.VariantType()
        case Struct(fields):
            return spark_types.StructType(
                [
                    spark_types.StructField(
                        str(field.name),
                        _to_spark_data_type(field.data_type),
                        nullable=field.nullable,
                    )
                    for field in fields
                ]
            )
        case _:
            raise TypeError(f"Unsupported DataType variant: {type(data_type).__name__}")


def to_spark_schema(table: DesiredTable) -> spark_types.StructType:
    """
    Return the desired table's data schema as a native PySpark ``StructType``.

    Column order, authored name spelling, data type, and nullability are
    preserved. Catalog annotations such as comments and tags are deliberately
    excluded: they are table metadata, not part of the DataFrame row schema.
    Array elements and map values are nullable because declarations do not
    model their nullability separately.
    """
    return spark_types.StructType(
        [
            spark_types.StructField(
                str(column.name),
                _to_spark_data_type(column.data_type),
                nullable=column.nullable,
            )
            for column in table.columns
        ]
    )
