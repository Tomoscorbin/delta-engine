import pytest

from tabula.adapters.databricks.sql.types import (
    sql_type_for_data_type,
    domain_type_from_uc,
    domain_type_from_spark,
)
from tabula.domain.model.data_type import DataType


@pytest.mark.parametrize(
    "name, expected_sql",
    [
        ("string", "STRING"),
        ("boolean", "BOOLEAN"),
        ("integer", "INT"),
        ("smallint", "SMALLINT"),
        ("bigint", "BIGINT"),
        ("float", "FLOAT"),
        ("double", "DOUBLE"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMP"),
    ],
)
def test_sql_type_for_data_type_scalar_mvp(name, expected_sql):
    assert sql_type_for_data_type(DataType(name)) == expected_sql


def test_sql_type_for_varchar_emits_string():
    assert sql_type_for_data_type(DataType("varchar", (255,))) == "STRING"


@pytest.mark.parametrize("uc, expected_domain", [
    ("INT", "integer"),
    ("INTEGER", "integer"),
    ("STRING", "string"),
    ("VARCHAR", "string"),
    ("DOUBLE PRECISION", "double"),
    ("BOOLEAN", "boolean"),
    ("DATE", "date"),
    ("TIMESTAMP", "timestamp"),
])
def test_domain_type_from_uc_scalars(uc, expected_domain):
    dt = domain_type_from_uc(uc)
    assert dt.name == expected_domain


@pytest.mark.parametrize("spark, expected_domain", [
    ("int", "integer"),
    ("integer", "integer"),
    ("string", "string"),
    ("double", "double"),
    ("boolean", "boolean"),
    ("date", "date"),
    ("timestamp", "timestamp"),
])
def test_domain_type_from_spark_scalars(spark, expected_domain):
    dt = domain_type_from_spark(spark)
    assert dt.name == expected_domain


def test_compiler_raises_on_unsupported_non_mvp():
    with pytest.raises(ValueError):
        sql_type_for_data_type(DataType("decimal", (18, 2)))
    with pytest.raises(ValueError):
        sql_type_for_data_type(DataType("array<int>"))
