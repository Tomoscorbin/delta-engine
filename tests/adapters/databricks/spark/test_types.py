"""Spark-parser round trips for domain type rendering."""

import json
from uuid import uuid4

from hypothesis import given, settings

from delta_engine.adapters.databricks.sql.types import data_type_from_json, render_data_type
from tests.adapters.databricks.sql.strategies import TYPE_CASES


@settings(max_examples=25, deadline=None)
@given(TYPE_CASES)
def test_rendered_types_parse_in_the_spark_sql_parser(spark, case) -> None:
    frame = spark.sql(f"SELECT CAST(NULL AS {render_data_type(case.data_type)}) AS value")

    assert frame.columns == ["value"]


@settings(max_examples=10, deadline=None)
@given(TYPE_CASES)
def test_rendered_types_round_trip_through_describe_as_json(spark, case) -> None:
    # Spark produces the DESCRIBE document here, so rendering and parsing must
    # agree through the real catalog with no hand-modelled JSON in between.
    # A parquet table, not Delta: DESCRIBE ... AS JSON rejects local v2 tables.
    table_name = f"render_round_trip_{uuid4().hex[:8]}"

    spark.sql(f"CREATE TABLE {table_name} (value {render_data_type(case.data_type)}) USING parquet")
    try:
        [row] = spark.sql(f"DESCRIBE EXTENDED {table_name} AS JSON").collect()
        document = json.loads(row[0])
        assert data_type_from_json(document["columns"][0]["type"]) == case.data_type
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
