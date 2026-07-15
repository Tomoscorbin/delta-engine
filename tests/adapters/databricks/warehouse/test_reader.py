import json

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_json_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.ports import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import Integer, QualifiedName, String

QN = QualifiedName("cat", "sch", "tbl")

_DOC = json.dumps(
    {
        "table_name": "tbl",
        "catalog_name": "cat",
        "schema_name": "sch",
        "columns": [
            {"name": "id", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
            {"name": "name", "type": {"name": "string"}, "nullable": True},
        ],
        "comment": "orders",
        "clustering_columns": ["id"],
        "table_properties": {
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "3",
        },
        "table_constraints": "[(pk_tbl,PRIMARY KEY (`id`))]",
    }
)


class RoutedCursor:
    def __init__(self, responses):
        self._responses = responses
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query):
        self.queries.append(query)
        value = self._responses.get(query)
        if isinstance(value, Exception):
            raise value
        self._current = value if value is not None else []

    def fetchone(self):
        return self._current[0] if self._current else None

    def fetchall(self):
        return list(self._current)


class RoutedConnection:
    def __init__(self, responses):
        self.cursor_fake = RoutedCursor(responses)

    def cursor(self):
        return self.cursor_fake


def _responses(describe=_DOC, **overrides):
    responses = {
        describe_json_query(QN): [(describe,)] if describe is not None else describe,
        table_tags_query(QN): [],
        column_tags_query(QN): [],
        referencing_foreign_keys_query(QN): [],
    }
    responses.update(overrides)
    return responses


def test_present_table_reads_via_as_json():
    connection = RoutedConnection(_responses())
    state = WarehouseReader(connection).fetch_state(QN)
    assert isinstance(state, TablePresent)
    observed = state.table
    assert [c.name for c in observed.columns] == ["id", "name"]
    assert observed.columns[0].data_type == Integer()
    assert observed.columns[1].data_type == String()
    assert observed.comment == "orders"
    assert observed.clustered_by == ("id",)
    assert dict(observed.properties) == {"delta.columnMapping.mode": "name"}
    assert observed.primary_key.columns == ("id",)


def test_present_table_uses_four_queries():
    connection = RoutedConnection(_responses())
    WarehouseReader(connection).fetch_state(QN)
    assert len(connection.cursor_fake.queries) == 4
    assert connection.cursor_fake.queries[0] == describe_json_query(QN)


def test_missing_table_is_absent_and_stops_after_describe():
    responses = {describe_json_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] nope")}
    connection = RoutedConnection(responses)
    assert isinstance(WarehouseReader(connection).fetch_state(QN), TableAbsent)
    assert connection.cursor_fake.queries == [describe_json_query(QN)]


def test_other_backend_error_is_read_failed():
    responses = {describe_json_query(QN): RuntimeError("warehouse gone")}
    state = WarehouseReader(RoutedConnection(responses)).fetch_state(QN)
    assert isinstance(state, ReadFailed)
    assert "warehouse gone" in state.failure.message
