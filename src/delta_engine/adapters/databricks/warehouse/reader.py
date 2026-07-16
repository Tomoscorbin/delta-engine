"""
Reader adapter for Databricks SQL warehouses.

Unity Catalog only. Reads one table's state through the shared
``read_catalog_state`` (one ``DESCRIBE TABLE EXTENDED … AS JSON`` plus five
information_schema queries). The connector is never imported at runtime: the
connection is duck-typed (``.cursor()`` yielding ``execute``/``fetchall``). One
cursor is acquired lazily on the first query and runs every statement — the
describe and the information_schema follow-ups — then closed when the read
finishes. Acquiring it lazily, inside the runner, keeps cursor acquisition
within the shared total boundary, so a dead connection becomes a ``ReadFailed``
rather than an escaping exception. This mirrors ``WarehouseExecutor.execute``.
"""

from __future__ import annotations

from collections.abc import Sequence
import contextlib
from typing import TYPE_CHECKING, Any

from delta_engine.adapters.databricks.read import read_catalog_state
from delta_engine.application.ports import CatalogState
from delta_engine.domain.model import QualifiedName

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor


class WarehouseReader:
    """Catalog state reader backed by a Databricks SQL warehouse connection."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """Return ``TablePresent``, ``TableAbsent``, or ``ReadFailed`` — the boundary is total."""
        cursor: Cursor | None = None

        def run_query(sql: str) -> Sequence[Any]:
            nonlocal cursor
            if cursor is None:
                cursor = self._connection.cursor()
            cursor.execute(sql)
            return cursor.fetchall()

        try:
            return read_catalog_state(run_query, qualified_name)
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    cursor.close()
