import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.spark.types import domain_type_from_spark
from delta_engine.adapters.databricks.sql.types import sql_type_for_data_type
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

pyspark = pytest.importorskip("pyspark")


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


def test_struct_with_casefold_colliding_field_names_is_unmappable() -> None:
    spark_struct = T.StructType(
        [
            T.StructField("Amount", T.IntegerType()),
            T.StructField("amount", T.StringType()),
        ]
    )
    assert domain_type_from_spark(spark_struct) is None


def test_leaf_types_round_trip_inside_collections() -> None:
    # Given leaf types nested in Array/Map, both mapping directions agree
    assert domain_type_from_spark(T.ArrayType(T.BinaryType())) == Array(Binary())
    assert domain_type_from_spark(T.MapType(T.ShortType(), T.TimestampNTZType())) == Map(
        Short(), TimestampNtz()
    )
    assert sql_type_for_data_type(Array(Binary())) == "ARRAY<BINARY>"
    assert sql_type_for_data_type(Map(Short(), TimestampNtz())) == "MAP<SMALLINT,TIMESTAMP_NTZ>"


def test_struct_maps_when_nested_in_structs_and_maps() -> None:
    # Given struct-in-struct and struct-in-map-value shapes
    inner = T.StructType([T.StructField("x", T.IntegerType())])
    spark_nested = T.StructType([T.StructField("inner", inner)])
    spark_in_map = T.MapType(T.StringType(), inner)

    domain_inner = Struct((StructField("x", Integer()),))

    # Then the mapping recurses in both directions
    assert domain_type_from_spark(spark_nested) == Struct((StructField("inner", domain_inner),))
    assert domain_type_from_spark(spark_in_map) == Map(String(), domain_inner)
    assert (
        sql_type_for_data_type(Struct((StructField("inner", domain_inner),)))
        == "STRUCT<`inner`: STRUCT<`x`: INT>>"
    )
