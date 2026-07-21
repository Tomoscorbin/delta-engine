from __future__ import annotations

import json

import pytest

from delta_engine.adapters.databricks.spark.reader import SparkReader
from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_json_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    schema_exists_query,
    table_tags_query,
)
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.domain.model import Integer, QualifiedName

QN = QualifiedName("cat", "sch", "tbl")

_DOC = json.dumps(
    {
        "table_name": "tbl",
        "catalog_name": "cat",
        "schema_name": "sch",
        "type": "MANAGED",
        "provider": "delta",
        "columns": [{"name": "id", "type": {"name": "int"}, "nullable": False}],
        "comment": "",
        "table_properties": {},
    }
)


class FakeAnalysisError(Exception):
    def __init__(self, condition):
        super().__init__(condition)
        self._condition = condition

    def getCondition(self):
        return self._condition


class FakeDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def collect(self):
        return list(self._rows)


class FakeSpark:
    """Routes spark.sql() by exact query text; the AS JSON result is one 1-col row."""

    def __init__(self, responses):
        self._responses = responses
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        value = self._responses.get(query, [])
        if isinstance(value, Exception):
            raise value
        return FakeDataFrame(value)


def _responses(describe=_DOC, **overrides):
    responses = {
        describe_json_query(QN): [(describe,)],
        table_tags_query(QN): [],
        column_tags_query(QN): [],
        primary_key_query(QN): [],
        foreign_keys_query(QN): [],
        referencing_foreign_keys_query(QN): [],
    }
    responses.update(overrides)
    return responses


def test_present_table_reads_via_as_json():
    spark = FakeSpark(_responses())
    state = SparkReader(spark).fetch_state(QN)
    assert isinstance(state, TablePresent)
    assert state.table.columns[0].data_type == Integer()


def test_present_table_uses_six_queries():
    spark = FakeSpark(_responses())
    SparkReader(spark).fetch_state(QN)
    assert len(spark.queries) == 6
    assert spark.queries[0] == describe_json_query(QN)


def test_missing_table_is_absent_after_confirming_the_schema_exists():
    spark = FakeSpark(
        {
            describe_json_query(QN): FakeAnalysisError("TABLE_OR_VIEW_NOT_FOUND"),
            schema_exists_query(QN): [("sch",)],
        }
    )
    assert isinstance(SparkReader(spark).fetch_state(QN), TableAbsent)
    assert spark.queries == [describe_json_query(QN), schema_exists_query(QN)]


def test_other_error_is_translated_to_read_error():
    spark = FakeSpark({describe_json_query(QN): FakeAnalysisError("INSUFFICIENT_PERMISSIONS")})

    with pytest.raises(ReadError) as exc_info:
        SparkReader(spark).fetch_state(QN)

    assert exc_info.value.exception_type == "FakeAnalysisError"
