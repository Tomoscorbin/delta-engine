from __future__ import annotations

import pytest
from types import SimpleNamespace

from tabula.adapters.databricks.sql.types import (
    sql_type_for_data_type,
    sql_type_for_column,
    domain_type_from_uc,
    domain_type_from_spark,
)
from tabula.domain.model import DataType


# ---------------- compiler mapping: primitives / happy paths ------------------

@pytest.mark.parametrize(
    ("logical", "expected_sql"),
    [
        ("string", "STRING"),
        ("boolean", "BOOLEAN"),
        ("tinyint", "TINYINT"),
        ("smallint", "SMALLINT"),
        ("integer", "INT"),
        ("bigint", "BIGINT"),
        ("float", "FLOAT"),
        ("double", "DOUBLE"),
        ("binary", "BINARY"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMP"),
        ("timestamp_ntz", "TIMESTAMP_NTZ"),
        ("variant", "VARIANT"),
    ],
)
def test_sql_type_for_data_type_primitives(logical: str, expected_sql: str) -> None:
    assert sql_type_for_data_type(DataType(logical)) == expected_sql


def test_sql_type_for_data_type_decimal_precision_scale() -> None:
    assert sql_type_for_data_type(DataType("decimal", (10, 2))) == "DECIMAL(10,2)"


def test_sql_type_for_data_type_decimal_precision_only_defaults_scale_zero() -> None:
    assert sql_type_for_data_type(DataType("decimal", (9,0))) == "DECIMAL(9,0)"
    assert sql_type_for_data_type(DataType("numeric", (12,0))) == "DECIMAL(12,0)"


def test_sql_type_for_data_type_varchar_maps_to_string() -> None:
    # length is informational only for UC/DBX
    assert sql_type_for_data_type(DataType("varchar", (255,))) == "STRING"


def test_sql_type_for_data_type_unsupported_raises_with_readable_spec() -> None:
    # arrays/maps/structs are unsupported in compiler MVP
    with pytest.raises(ValueError) as err:
        sql_type_for_data_type(DataType("array", (DataType("integer"),)))
    msg = str(err.value).lower()
    # message should include the rendered spec (no reliance on a .specification attr)
    assert "unsupported" in msg and "array(integer)" in msg


# ---------------- sql_type_for_column precedence / errors ---------------------

def test_sql_type_for_column_prefers_pre_mapped_sql_type() -> None:
    col = SimpleNamespace(sql_type="DECIMAL(12,3)", data_type=DataType("integer"))
    assert sql_type_for_column(col) == "DECIMAL(12,3)"


def test_sql_type_for_column_falls_back_to_data_type() -> None:
    col = SimpleNamespace(data_type=DataType("integer"))
    assert sql_type_for_column(col) == "INT"


def test_sql_type_for_column_missing_both_raises() -> None:
    col = SimpleNamespace()
    with pytest.raises(ValueError, match="neither 'sql_type' nor 'data_type'"):
        sql_type_for_column(col)


# ---------------- UC reader (tolerant) ---------------------------------------

@pytest.mark.parametrize(
    ("uc_text", "expected"),
    [
        ("STRING", DataType("string")),
        ("VARCHAR", DataType("string")),
        ("BOOLEAN", DataType("boolean")),
        ("TINYINT", DataType("tinyint")),
        ("SMALLINT", DataType("smallint")),
        ("INT", DataType("integer")),
        ("INTEGER", DataType("integer")),
        ("BIGINT", DataType("bigint")),
        ("FLOAT", DataType("float")),
        ("REAL", DataType("float")),
        ("DOUBLE", DataType("double")),
        ("DOUBLE PRECISION", DataType("double")),
        ("BINARY", DataType("binary")),
        ("DATE", DataType("date")),
        ("TIMESTAMP", DataType("timestamp")),
        ("TIMESTAMP_NTZ", DataType("timestamp_ntz")),
        ("VARIANT", DataType("variant")),
    ],
)
def test_domain_type_from_uc_scalars(uc_text: str, expected: DataType) -> None:
    assert domain_type_from_uc(uc_text) == expected


def test_domain_type_from_uc_decimal_is_parsed() -> None:
    assert domain_type_from_uc("DECIMAL(18, 2)") == DataType("decimal", (18, 2))


def test_domain_type_from_uc_timestamp_ntz_with_precision_prefix_is_supported() -> None:
    # Some systems include precision, but we only care that it signals NTZ
    assert domain_type_from_uc("TIMESTAMP_NTZ(9)") == DataType("timestamp_ntz")


# ---------------- Spark reader (tolerant) ------------------------------------

@pytest.mark.parametrize(
    ("spark_text", "expected"),
    [
        ("string", DataType("string")),
        ("boolean", DataType("boolean")),
        ("tinyint", DataType("tinyint")),
        ("byte", DataType("tinyint")),
        ("smallint", DataType("smallint")),
        ("int", DataType("integer")),        # reader normalizes to 'integer'
        ("integer", DataType("integer")),
        ("bigint", DataType("bigint")),
        ("float", DataType("float")),
        ("real", DataType("float")),
        ("double", DataType("double")),
        ("binary", DataType("binary")),
        ("date", DataType("date")),
        ("timestamp", DataType("timestamp")),
        ("timestamp_ntz", DataType("timestamp_ntz")),
    ],
)
def test_domain_type_from_spark_scalars(spark_text: str, expected: DataType) -> None:
    assert domain_type_from_spark(spark_text) == expected


def test_domain_type_from_spark_decimal_is_parsed() -> None:
    assert domain_type_from_spark("decimal(10,2)") == DataType("decimal", (10, 2))


def test_domain_type_from_spark_passthrough_for_arrays_and_structs() -> None:
    assert domain_type_from_spark("array<int>") == DataType("array<int>")
