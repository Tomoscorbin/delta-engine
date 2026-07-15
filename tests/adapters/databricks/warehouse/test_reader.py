"""Shell tests for the SQL warehouse catalog-state reader."""

import json

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_detail_query,
    describe_json_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.ports import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.data_type import Integer, String, Struct, StructField

QN = QualifiedName("cat", "sch", "tbl")


def described_table_json(**overrides: object) -> str:
    document = {
        "table_name": "tbl",
        "catalog_name": "cat",
        "schema_name": "sch",
        "type": "MANAGED",
        "provider": "delta",
        "columns": [
            {"name": "id", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
            {"name": "name", "type": {"name": "string"}, "nullable": True},
        ],
        "comment": "orders",
    }
    document.update(overrides)
    return json.dumps(document)


class DetailRow(dict):
    """Duck-typed DESCRIBE DETAIL row: attribute access plus asDict()."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as error:  # pragma: no cover - defensive
            raise AttributeError(name) from error

    def asDict(self):
        return dict(self)


def detail_row(properties: str | None = "{}", **extra: object) -> DetailRow:
    return DetailRow(properties=properties, **extra)


class RoutedCursor:
    """Cursor fake answering reads from an exact query-text table."""

    def __init__(self, responses: dict):
        self._responses = responses
        self._current = None
        self.queries: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query: str) -> None:
        self.queries.append(query)
        if query not in self._responses:
            raise AssertionError(f"unexpected query: {query}")
        value = self._responses[query]
        if isinstance(value, Exception):
            raise value
        self._current = value

    def fetchone(self):
        return self._current[0] if self._current else None

    def fetchall(self):
        return list(self._current)


class RoutedConnection:
    def __init__(self, responses: dict):
        self.cursor_fake = RoutedCursor(responses)

    def cursor(self):
        return self.cursor_fake


def routed_connection(
    *,
    description: str | None = None,
    detail=None,
    pk=(),
    fks=(),
    referencing=(),
    table_tags=(),
    column_tags=(),
) -> RoutedConnection:
    """Wire one successful table read with overridable metadata responses."""
    responses = {
        describe_json_query(QN): [(description or described_table_json(),)],
        describe_detail_query(QN): [detail if detail is not None else detail_row()],
        primary_key_query(QN): list(pk),
        foreign_keys_query(QN): list(fks),
        referencing_foreign_keys_query(QN): list(referencing),
        table_tags_query(QN): list(table_tags),
        column_tags_query(QN): list(column_tags),
    }
    return RoutedConnection(responses)


def fetch_present(connection) -> TablePresent:
    state = WarehouseReader(connection).fetch_state(QN)
    assert isinstance(state, TablePresent), state
    return state


def test_missing_table_error_means_absent_and_stops_reading():
    connection = RoutedConnection(
        {describe_json_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] missing")}
    )

    assert isinstance(WarehouseReader(connection).fetch_state(QN), TableAbsent)
    assert connection.cursor_fake.queries == [describe_json_query(QN)]


def test_present_table_uses_json_for_columns_comment_and_partitioning():
    description = described_table_json(
        columns=[
            {"name": "ID", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
            {"name": "name", "type": {"name": "string"}, "nullable": True},
            {"name": "Region", "type": {"name": "string"}, "nullable": True},
        ],
        comment="orders table",
        partition_columns=["Region"],
    )

    observed = fetch_present(routed_connection(description=description)).table

    assert [column.name for column in observed.columns] == ["id", "name", "region"]
    assert observed.columns[0].data_type == Integer()
    assert observed.columns[0].nullable is False
    assert observed.columns[0].comment == "pk"
    assert observed.columns[1].data_type == String()
    assert observed.comment == "orders table"
    assert observed.partitioned_by == ("region",)


def test_structured_type_preserves_special_character_struct_field_names():
    description = described_table_json(
        columns=[
            {
                "name": "payload",
                "type": {
                    "name": "struct",
                    "fields": [{"name": "bad name", "type": {"name": "int"}}],
                },
                "nullable": True,
            }
        ]
    )

    [column] = fetch_present(routed_connection(description=description)).table.columns

    assert column.data_type == Struct((StructField("bad name", Integer()),))


def test_properties_and_clustering_still_come_from_describe_detail():
    description = described_table_json(
        table_properties={"delta.columnMapping.mode": "not-used"},
        clustering_columns=["not_used"],
    )
    detail = detail_row(
        properties='{"delta.columnMapping.mode": "name", "delta.internal.noise": "x"}',
        clusteringColumns='["ID"]',
    )

    observed = fetch_present(routed_connection(description=description, detail=detail)).table

    assert dict(observed.properties) == {"delta.columnMapping.mode": "name"}
    assert observed.clustered_by == ("id",)


def test_keys_and_tags_still_come_from_information_schema():
    description = described_table_json(table_constraints="[(ignored,PRIMARY KEY (`name`))]")
    connection = routed_connection(
        description=description,
        pk=[SimpleRow(constraint_name="PK_TBL", column_name="ID")],
        table_tags=[SimpleRow(tag_name="Owner", tag_value="Data")],
        column_tags=[SimpleRow(column_name="ID", tag_name="pii", tag_value="low")],
    )

    observed = fetch_present(connection).table

    assert observed.primary_key is not None
    assert observed.primary_key.constraint_name == "pk_tbl"
    assert observed.primary_key.columns == ("id",)
    assert dict(observed.tags) == {"Owner": "Data"}
    assert dict(observed.columns[0].tags) == {"pii": "low"}


class SimpleRow:
    def __init__(self, **values: object):
        self.__dict__.update(values)


def test_present_table_uses_seven_reads_with_json_first():
    connection = routed_connection()

    fetch_present(connection)

    assert len(connection.cursor_fake.queries) == 7
    assert connection.cursor_fake.queries[0] == describe_json_query(QN)


def test_view_and_non_delta_table_fail_closed():
    for description in (
        described_table_json(type="VIEW"),
        described_table_json(provider="parquet"),
    ):
        state = WarehouseReader(routed_connection(description=description)).fetch_state(QN)
        assert isinstance(state, ReadFailed)


def test_unmappable_non_partition_column_is_skipped():
    description = described_table_json(
        columns=[
            {"name": "ok", "type": {"name": "int"}, "nullable": True},
            {"name": "weird", "type": {"name": "geography"}, "nullable": True},
        ]
    )
    observed = fetch_present(routed_connection(description=description)).table
    assert [column.name for column in observed.columns] == ["ok"]


def test_unmappable_partition_column_fails_the_read():
    description = described_table_json(
        partition_columns=["p"],
        columns=[{"name": "p", "type": {"name": "geography"}, "nullable": True}],
    )
    state = WarehouseReader(routed_connection(description=description)).fetch_state(QN)
    assert isinstance(state, ReadFailed)
    assert "partition" in state.failure.message.casefold()


def test_empty_describe_json_result_fails_the_read():
    connection = routed_connection()
    connection.cursor_fake._responses[describe_json_query(QN)] = []
    assert isinstance(WarehouseReader(connection).fetch_state(QN), ReadFailed)


def test_empty_describe_detail_fails_the_read():
    connection = routed_connection()
    connection.cursor_fake._responses[describe_detail_query(QN)] = []
    assert isinstance(WarehouseReader(connection).fetch_state(QN), ReadFailed)


def test_unexpected_backend_exception_becomes_read_failed():
    connection = RoutedConnection({describe_json_query(QN): RuntimeError("warehouse gone")})
    state = WarehouseReader(connection).fetch_state(QN)
    assert isinstance(state, ReadFailed)
    assert state.failure.exception_type == "RuntimeError"
    assert "warehouse gone" in state.failure.message


def test_exception_with_an_unrenderable_message_becomes_read_failed():
    class UnrenderableError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("rendering failed")

    connection = RoutedConnection({describe_json_query(QN): UnrenderableError()})
    state = WarehouseReader(connection).fetch_state(QN)

    assert isinstance(state, ReadFailed)
    assert state.failure.exception_type == "UnrenderableError"
    assert state.failure.message == "<exception message unavailable>"
