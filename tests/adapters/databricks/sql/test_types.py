import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.sql.types import (
    domain_type_from_spark,
    sql_type_for_data_type,
)
from delta_engine.domain.model.data_type import (
    Array,
    Binary,
    Boolean,
    Byte,
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

pyspark = pytest.importorskip("pyspark")  # TODO: create sparkSession fixture


def test_sql_type_for_primitive_types() -> None:
    assert sql_type_for_data_type(Integer()) == "INT"
    assert sql_type_for_data_type(Long()) == "BIGINT"
    assert sql_type_for_data_type(Byte()) == "TINYINT"
    assert sql_type_for_data_type(Short()) == "SMALLINT"
    assert sql_type_for_data_type(Float()) == "FLOAT"
    assert sql_type_for_data_type(Double()) == "DOUBLE"
    assert sql_type_for_data_type(Boolean()) == "BOOLEAN"
    assert sql_type_for_data_type(String()) == "STRING"
    assert sql_type_for_data_type(Binary()) == "BINARY"
    assert sql_type_for_data_type(Date()) == "DATE"
    assert sql_type_for_data_type(Timestamp()) == "TIMESTAMP"
    assert sql_type_for_data_type(TimestampNtz()) == "TIMESTAMP_NTZ"
    assert sql_type_for_data_type(Variant()) == "VARIANT"


def test_sql_type_for_decimal_array_map_recursive() -> None:
    assert sql_type_for_data_type(Decimal(10, 2)) == "DECIMAL(10,2)"
    assert sql_type_for_data_type(Array(String())) == "ARRAY<STRING>"
    assert sql_type_for_data_type(Map(String(), Integer())) == "MAP<STRING,INT>"
    # nested
    nested = Array(Map(String(), Decimal(9, 0)))
    assert sql_type_for_data_type(nested) == "ARRAY<MAP<STRING,DECIMAL(9,0)>>"


def test_domain_type_from_spark_returns_none_for_unmappable_type() -> None:
    # Given a Spark type the engine does not map
    # Then the conversion returns None rather than raising
    assert domain_type_from_spark(T.NullType()) is None


def test_domain_type_from_spark_maps_primitives() -> None:
    # Given each Spark primitive the engine supports
    # Then it maps to the matching domain type
    assert domain_type_from_spark(T.IntegerType()) == Integer()
    assert domain_type_from_spark(T.LongType()) == Long()
    assert domain_type_from_spark(T.ByteType()) == Byte()
    assert domain_type_from_spark(T.ShortType()) == Short()
    assert domain_type_from_spark(T.FloatType()) == Float()
    assert domain_type_from_spark(T.DoubleType()) == Double()
    assert domain_type_from_spark(T.BooleanType()) == Boolean()
    assert domain_type_from_spark(T.StringType()) == String()
    assert domain_type_from_spark(T.BinaryType()) == Binary()
    assert domain_type_from_spark(T.DateType()) == Date()
    assert domain_type_from_spark(T.TimestampType()) == Timestamp()
    assert domain_type_from_spark(T.TimestampNTZType()) == TimestampNtz()
    assert domain_type_from_spark(T.VariantType()) == Variant()


def test_domain_type_from_spark_maps_decimal_and_nested_collections() -> None:
    # Given decimal, array, and map types (including nesting)
    # Then the mapping recurses into element and key/value types
    assert domain_type_from_spark(T.DecimalType(12, 3)) == Decimal(12, 3)
    assert domain_type_from_spark(T.ArrayType(T.StringType())) == Array(String())
    assert domain_type_from_spark(T.MapType(T.StringType(), T.IntegerType())) == Map(
        String(), Integer()
    )
    nested = T.ArrayType(T.MapType(T.StringType(), T.DecimalType(9, 0)))
    assert domain_type_from_spark(nested) == Array(Map(String(), Decimal(9, 0)))


def test_domain_type_from_spark_normalises_char_and_varchar_to_string() -> None:
    # CHAR/VARCHAR length limits are invisible to the engine: observed as
    # String, they produce no drift and are never altered.
    assert domain_type_from_spark(T.VarcharType(10)) == String()
    assert domain_type_from_spark(T.CharType(5)) == String()


def test_domain_type_from_spark_returns_none_when_collection_element_is_unmappable() -> None:
    # Given a collection whose element type has no domain mapping
    # Then the whole type is unmappable
    assert domain_type_from_spark(T.ArrayType(T.NullType())) is None
    assert domain_type_from_spark(T.MapType(T.StringType(), T.NullType())) is None


def test_sql_type_for_struct_renders_fields_in_order() -> None:
    struct = Struct((StructField("a", Integer()), StructField("b", Array(String()))))
    assert sql_type_for_data_type(struct) == "STRUCT<a: INT, b: ARRAY<STRING>>"


def test_domain_type_from_spark_maps_struct_and_casefolds_field_names() -> None:
    spark_struct = T.StructType(
        [
            T.StructField("Amount", T.DecimalType(10, 2)),
            T.StructField("note", T.StringType()),
        ]
    )
    assert domain_type_from_spark(spark_struct) == Struct(
        (StructField("amount", Decimal(10, 2)), StructField("note", String()))
    )


def test_domain_type_from_spark_returns_none_for_struct_with_unmappable_field() -> None:
    spark_struct = T.StructType([T.StructField("x", T.NullType())])
    assert domain_type_from_spark(spark_struct) is None


def test_domain_type_from_spark_returns_none_for_struct_with_casefold_colliding_field_names() -> (
    None
):
    spark_struct = T.StructType(
        [
            T.StructField("Amount", T.IntegerType()),
            T.StructField("amount", T.StringType()),
        ]
    )
    assert domain_type_from_spark(spark_struct) is None
