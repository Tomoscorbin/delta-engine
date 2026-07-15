"""
Reader adapter for Databricks SQL warehouses.

Unity Catalog only. One ``DESCRIBE TABLE EXTENDED … AS JSON`` yields the
table-local state; three information_schema queries add tags and inbound
foreign keys. The connector is never imported at runtime: the connection is
duck-typed (``.cursor()`` context manager with ``execute``/``fetchone``/
``fetchall``), so this backend imports nothing beyond the shared adapter core.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from delta_engine.adapters.databricks.errors import (
    exception_message,
    exception_type_name,
    is_missing_relation,
)
from delta_engine.adapters.databricks.read import observed_table_from_snapshot
from delta_engine.adapters.databricks.sql import describe_json_query
from delta_engine.adapters.databricks.sql.describe_json import parse_table_snapshot
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName

if TYPE_CHECKING:
    from databricks.sql.client import Connection


class WarehouseReader:
    """Catalog state reader backed by a Databricks SQL warehouse connection."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """Return ``TablePresent``, ``TableAbsent``, or ``ReadFailed`` — the boundary is total."""
        try:
            return self._read(qualified_name)
        except Exception as exception:
            return ReadFailed(
                failure=ReadFailure(exception_type_name(exception), exception_message(exception))
            )

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        with self._connection.cursor() as cursor:
            try:
                cursor.execute(describe_json_query(qualified_name))
            except Exception as exception:
                if is_missing_relation(exception):
                    return TableAbsent()
                raise
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"DESCRIBE AS JSON returned no row for {qualified_name}")
            snapshot = parse_table_snapshot(row[0], qualified_name)
            observed = observed_table_from_snapshot(
                snapshot, run_info_schema_query=lambda query: _fetch_all(cursor, query)
            )
        return TablePresent(table=observed)


def _fetch_all(cursor: Any, query: str) -> list[Any]:
    cursor.execute(query)
    return cursor.fetchall()
