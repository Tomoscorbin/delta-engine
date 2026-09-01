"""
Live pins for the read boundary's relation acceptance.

The engine reads managed and external Delta tables, and streaming tables, as
observed state. Each test creates a catalog relation outside that set and
asserts the engine's own ``WarehouseReader`` fails the read rather than
admitting it as a table to diff and plan against. (Streaming tables are
pinned positively in ``test_sql_warehouse_live_streaming_tables.py``.)
"""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import CatalogState
from delta_engine.domain.model import QualifiedName
from tests.live.capabilities import require_databricks_capability
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
)

_ICEBERG_UNAVAILABLE_CONDITIONS = {
    "UC_DATASOURCE_NOT_SUPPORTED",
    "UC_MANAGED_ICEBERG_UNSUPPORTED",
    "UNSUPPORTED_MANAGED_TABLE_CREATION",
}


def _read(live_connection, table_name: str) -> CatalogState:
    reader = WarehouseReader(WarehouseSqlRunner(live_connection))
    return reader.fetch_state(QualifiedName(live_catalog(), live_schema(), table_name))


def test_a_view_is_not_read_as_a_table(live_connection, live_tables):
    """A view fails the read instead of being observed as a table."""
    # Given a live view over an ordinary Delta table
    table_name = live_tables("relation_base")
    view_name = live_tables("relation_view")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (id INT) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"CREATE VIEW {qualified_table(view_name)} AS SELECT id FROM {qualified_table(table_name)}",
    )

    try:
        # Then reading the view fails as an unsupported relation
        with pytest.raises(ReadError) as exc_info:
            _read(live_connection, view_name)

        assert exc_info.value.exception_type == "UnsupportedRelationError"
    finally:
        execute_sql(live_connection, f"DROP VIEW IF EXISTS {qualified_table(view_name)}")


def test_a_non_delta_table_is_not_read_as_engine_state(live_connection, live_tables):
    """An Iceberg table fails the read: the engine reads Delta tables only."""
    # Given an ordinary managed table in a format the engine's DDL cannot manage
    table_name = live_tables("relation_iceberg")
    require_databricks_capability(
        lambda: execute_sql(
            live_connection,
            f"CREATE TABLE {qualified_table(table_name)} (id INT) USING ICEBERG",
        ),
        capability="managed Iceberg tables",
        unavailable_conditions=_ICEBERG_UNAVAILABLE_CONDITIONS,
    )

    # Then reading the non-Delta relation rejects it instead of treating it
    # as manageable state
    with pytest.raises(ReadError) as exc_info:
        _read(live_connection, table_name)

    assert exc_info.value.exception_type == "UnsupportedRelationError"
