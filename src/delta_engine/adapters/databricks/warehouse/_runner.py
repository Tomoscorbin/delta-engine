"""Physical SQL invocation through a Databricks SQL warehouse connection."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
import logging
from typing import TYPE_CHECKING, Any

from delta_engine.adapters.databricks.sql import RunQuery

if TYPE_CHECKING:
    from databricks.sql.client import Connection, Cursor


logger = logging.getLogger(__name__)


class WarehouseSqlRunner:
    """Run SQL while containing warehouse cursor lifecycle policy."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    @contextmanager
    def query_batch(self) -> Iterator[RunQuery]:
        """Yield a query callable that reuses one cursor for the batch."""
        with self._cursor_scope() as cursor:
            yield cursor.query

    def run(self, statement: str) -> None:
        """Execute one statement with a fresh cursor scope."""
        with self._cursor_scope() as cursor:
            cursor.execute(statement)

    @contextmanager
    def _cursor_scope(self) -> Iterator[_CursorScope]:
        cursor = _CursorScope(self._connection)
        try:
            yield cursor
        finally:
            cursor.close()


class _CursorScope:
    """Lazily acquire and reliably close one warehouse cursor."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._cursor: Cursor | None = None

    def query(self, statement: str) -> Sequence[Any]:
        """Execute a query and return all of its rows."""
        cursor = self._get_cursor()
        cursor.execute(statement)
        return cursor.fetchall()

    def execute(self, statement: str) -> None:
        """Execute a statement without fetching rows."""
        self._get_cursor().execute(statement)

    def close(self) -> None:
        """Close an acquired cursor without replacing the operation outcome."""
        if self._cursor is None:
            return
        try:
            self._cursor.close()
        except Exception:
            logger.debug("Failed to close warehouse cursor", exc_info=True)

    def _get_cursor(self) -> Cursor:
        if self._cursor is None:
            self._cursor = self._connection.cursor()
        return self._cursor
