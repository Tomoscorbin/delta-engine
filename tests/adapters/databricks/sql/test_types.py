import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.sql.types import (
    domain_type_from_spark,
    sql_type_for_data_type,
)
from delta_engine.domain.model.data_type import (
    Array,
    Boolean,
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

pyspark = pytest.importorskip("pyspark")  # TODO: create sparkSession fixture


def test_sql_type_for_primitive_types() -> None:
    assert sql_type_for_data_type(Integer()) == "INT"
    assert sql_type_for_data_type(Long()) == "BIGINT"
    assert sql_type_for_data_type(Float()) == "FLOAT"
    assert sql_type_for_data_type(Double()) == "DOUBLE"
    assert sql_type_for_data_type(Boolean()) == "BOOLEAN"
    assert sql_type_for_data_type(String()) == "STRING"
    assert sql_type_for_data_type(Date()) == "DATE"
    assert sql_type_for_data_type(Timestamp()) == "TIMESTAMP"


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
    assert domain_type_from_spark(T.BinaryType()) is None


def test_domain_type_from_spark_maps_primitives() -> None:
    # Given each Spark primitive the engine supports
    # Then it maps to the matching domain type
    assert domain_type_from_spark(T.IntegerType()) == Integer()
    assert domain_type_from_spark(T.LongType()) == Long()
    assert domain_type_from_spark(T.FloatType()) == Float()
    assert domain_type_from_spark(T.DoubleType()) == Double()
    assert domain_type_from_spark(T.BooleanType()) == Boolean()
    assert domain_type_from_spark(T.StringType()) == String()
    assert domain_type_from_spark(T.DateType()) == Date()
    assert domain_type_from_spark(T.TimestampType()) == Timestamp()


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


def test_domain_type_from_spark_returns_none_when_collection_element_is_unmappable() -> None:
    # Given a collection whose element type has no domain mapping
    # Then the whole type is unmappable
    assert domain_type_from_spark(T.ArrayType(T.BinaryType())) is None
    assert domain_type_from_spark(T.MapType(T.StringType(), T.BinaryType())) is None
