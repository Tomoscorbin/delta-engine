"""WarehouseReader drives the shared catalog read through one cursor batch."""

import pytest

from delta_engine.adapters.databricks.sql import describe_json_query, schema_exists_query
from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.domain.model import Integer, QualifiedName, String
from tests.adapters.databricks.fakes import (
    ClosedConnection,
    RoutedConnection,
    build_catalog_responses,
    build_describe_document,
)

QN = QualifiedName("cat", "sch", "tbl")


def _reader(connection) -> WarehouseReader:
    return WarehouseReader(WarehouseSqlRunner(connection))


_DOC = build_describe_document(
    QN,
    columns=[
        {"name": "id", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
        {"name": "name", "type": {"name": "string"}, "nullable": True},
    ],
    comment="orders",
    clustering_columns=["id"],
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "3",
    },
    table_constraints="[(pk_tbl,PRIMARY KEY (`id`))]",
)


def test_present_table_reads_via_as_json():
    # Given a described table reachable through a warehouse connection
    connection = RoutedConnection(build_catalog_responses(QN, describe=_DOC))

    state = _reader(connection).fetch_state(QN)

    # Then the described state reaches the observed table through the cursor
    assert isinstance(state, TablePresent)
    observed = state.table
    assert [c.name for c in observed.columns] == ["id", "name"]
    assert observed.columns[0].data_type == Integer()
    assert observed.columns[1].data_type == String()
    assert observed.comment == "orders"
    assert observed.clustered_by == ("id",)
    assert dict(observed.properties) == {"delta.columnMapping.mode": "name"}
    # The table_constraints embedded in the AS JSON doc is not read: the primary
    # key comes from information_schema, which returns nothing here.
    assert observed.primary_key is None


def test_missing_table_is_absent_after_confirming_the_schema_exists():
    # Given a missing table inside an existing schema
    responses = {
        describe_json_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] nope"),
        schema_exists_query(QN): [("sch",)],
    }
    connection = RoutedConnection(responses)

    # Then the table reads as absent, probing the schema second
    assert isinstance(_reader(connection).fetch_state(QN), TableAbsent)
    assert connection.cursor_fake.queries == [describe_json_query(QN), schema_exists_query(QN)]


def test_other_backend_error_is_translated_to_read_error():
    # Given a warehouse backend failure on the describe
    responses = {describe_json_query(QN): RuntimeError("warehouse gone")}

    with pytest.raises(ReadError) as exc_info:
        _reader(RoutedConnection(responses)).fetch_state(QN)

    # Then the failure surfaces as a read error
    assert "warehouse gone" in str(exc_info.value)


def test_cursor_acquisition_failure_is_translated_to_read_error():
    # Given a connection that cannot open a cursor
    with pytest.raises(ReadError) as exc_info:
        _reader(ClosedConnection()).fetch_state(QN)

    # Then the acquisition failure is a read error too
    assert "closed connection" in str(exc_info.value)


def test_cursor_is_closed_after_a_successful_read():
    # Given a complete read through one connection
    connection = RoutedConnection(build_catalog_responses(QN))

    _reader(connection).fetch_state(QN)

    # Then the cursor does not leak
    assert connection.cursor_fake.closed is True


def test_cursor_is_closed_when_the_read_fails():
    # Given a read that fails mid-batch
    responses = {describe_json_query(QN): RuntimeError("warehouse gone")}
    connection = RoutedConnection(responses)

    with pytest.raises(ReadError):
        _reader(connection).fetch_state(QN)

    # Then the cursor still does not leak
    assert connection.cursor_fake.closed is True
