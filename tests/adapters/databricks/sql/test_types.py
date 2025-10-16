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


def test_domain_type_from_spark_primitives_via_strings(spark) -> None:
    # Using DDL strings exercises the fromDDL path
    assert domain_type_from_spark("int") == Integer()
    assert domain_type_from_spark("bigint") == Long()
    assert domain_type_from_spark("float") == Float()
    assert domain_type_from_spark("double") == Double()
    assert domain_type_from_spark("boolean") == Boolean()
    assert domain_type_from_spark("string") == String()
    assert domain_type_from_spark("date") == Date()
    assert domain_type_from_spark("timestamp") == Timestamp()


def test_domain_type_from_spark_decimal_array_map_recursive(spark) -> None:
    assert domain_type_from_spark("decimal(12,3)") == Decimal(12, 3)
    assert domain_type_from_spark("array<string>") == Array(String())
    assert domain_type_from_spark("map<string,int>") == Map(String(), Integer())
    assert domain_type_from_spark("array<map<string,decimal(9,0)>>") == Array(
        Map(String(), Decimal(9, 0))
    )
