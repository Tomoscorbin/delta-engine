"""
Map parsed ``pyspark.sql.types`` instances to domain types.

The read half of the adapter's type mapping: the reader parses catalog DDL
text with ``SparkType.fromDDL`` and this module maps the resulting instances
into domain types. The write half (domain type to DDL string) is PySpark-free
and lives in :mod:`delta_engine.adapters.databricks.sql.types`.

Uses ``match``/``case`` rather than ``functools.singledispatch`` (which the plan
compiler uses): ``DataType`` is a closed set and the mapping is a leaf lookup,
where structural patterns like ``case DecimalType()`` destructure fields
inline. ``singledispatch`` fits the compiler because the ``Action`` hierarchy is
open to extension; it would only add ceremony here.
"""

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    CharType,
    DataType as SparkType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
    VarcharType,
    VariantType,
)

from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
    DataType,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Map,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
    Variant,
)


def domain_type_from_spark(spark_type: SparkType) -> DataType | None:
    """
    Map a ``pyspark`` type instance to a domain type.

    Returns ``None`` when the type has no domain mapping (e.g. ``VOID``,
    ``INTERVAL``). An unmappable element inside an ``ARRAY``, ``MAP``, or
    ``STRUCT`` makes the whole type unmappable, as do struct field names that
    collide after casefolding. An unmappable type is a routine,
    expected condition -- new Spark types appear over time -- so it is a
    ``None`` return, not an exception. Callers decide what to do with ``None``
    (the reader skips the column and logs a warning).

    Takes an already-parsed type instance, not a DDL string: parsing catalog DDL
    text is the reader's concern (``SparkType.fromDDL``), kept out of this
    module. Operating on instances also means the whole mapping table needs no
    ``SparkSession`` to exercise.
    """
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
        case ByteType():
            return Byte()
        case ShortType():
            return Short()
        case BinaryType():
            return Binary()
        case TimestampNTZType():
            return TimestampNtz()
        case VariantType():
            return Variant()
        case CharType() | VarcharType():
            # Lossy normalization: the length bound is not modeled, so the
            # engine sees these as plain strings, plans no change for them,
            # and never emits CHAR/VARCHAR in DDL.
            return String()
        case StructType():
            fields: list[StructField] = []
            seen_names: set[str] = set()
            for spark_field in spark_type.fields:
                field_type = domain_type_from_spark(spark_field.dataType)
                if field_type is None:
                    return None
                name = spark_field.name.casefold()
                if name in seen_names:
                    # Field names that collide after casefolding cannot be
                    # represented in the domain model, so the struct is
                    # unmappable rather than a constructor error.
                    return None
                seen_names.add(name)
                fields.append(StructField(name=name, data_type=field_type))
            return Struct(tuple(fields))
        case _:
            return None
