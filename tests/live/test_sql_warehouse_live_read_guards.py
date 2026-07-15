"""
Live coverage for the read boundary's relation-kind and format guards.

Each test creates an unsupported catalog object and asserts the engine's own
WarehouseReader returns ReadFailed rather than admitting it as observed state.
"""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.ports import CatalogState, ReadFailed
from delta_engine.domain.model import QualifiedName
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
)


def _read(live_connection, table_name: str) -> CatalogState:
    reader = WarehouseReader(live_connection)
    return reader.fetch_state(QualifiedName(live_catalog(), live_schema(), table_name))


def test_view_is_rejected_at_the_read_boundary(live_connection, live_tables):
    """A view is rejected at the read boundary rather than read as a table."""
    table_name = live_tables("guard_base")
    view_name = live_tables("guard_view")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (id INT) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"CREATE VIEW {qualified_table(view_name)} AS SELECT id FROM {qualified_table(table_name)}",
    )

    try:
        state = _read(live_connection, view_name)

        assert isinstance(state, ReadFailed)
        assert state.failure.exception_type == "UnsupportedCatalogRelationError"
    finally:
        execute_sql(live_connection, f"DROP VIEW IF EXISTS {qualified_table(view_name)}")


def test_streaming_table_is_rejected_at_the_read_boundary(live_connection, live_tables):
    """A streaming table is rejected at the read boundary despite reporting Delta format."""
    # A streaming table reports format='delta'; only the relation-kind guard
    # rejects it. Skip cleanly if the workspace cannot create one.
    table_name = live_tables("guard_stream")
    try:
        execute_sql(
            live_connection,
            f"CREATE STREAMING TABLE {qualified_table(table_name)} AS SELECT 1 AS id",
        )
    except Exception as exc:  # intentional broad except: environment capability probe
        pytest.skip(f"workspace cannot create a streaming table here: {exc}")

    try:
        state = _read(live_connection, table_name)

        assert isinstance(state, ReadFailed)
        assert state.failure.exception_type == "UnsupportedCatalogRelationError"
    finally:
        execute_sql(
            live_connection, f"DROP STREAMING TABLE IF EXISTS {qualified_table(table_name)}"
        )


def test_non_delta_format_is_rejected_at_the_read_boundary(live_connection, live_tables):
    """A non-Delta (Iceberg) table is rejected at the read boundary by the format guard."""
    # DESCRIBE DETAIL succeeds on an Iceberg table and reports format='iceberg',
    # so the format guard — not the relation-kind guard — rejects it. The
    # live_tables fixture drops it with DROP TABLE, which is correct for an
    # Iceberg table. Skip cleanly if the workspace cannot create one.
    table_name = live_tables("guard_iceberg")
    try:
        execute_sql(
            live_connection,
            f"CREATE TABLE {qualified_table(table_name)} (id INT) USING ICEBERG",
        )
    except Exception as exc:  # intentional broad except: environment capability probe
        pytest.skip(f"workspace cannot create an Iceberg table here: {exc}")

    state = _read(live_connection, table_name)

    assert isinstance(state, ReadFailed), state
    assert state.failure.exception_type == "UnsupportedCatalogRelationError", state.failure
