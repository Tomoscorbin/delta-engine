"""
Reader adapter for Databricks SQL warehouses.

Unity Catalog only. ``DESCRIBE TABLE EXTENDED … AS JSON`` supplies relation,
provider, column, comment, and partition metadata; ``DESCRIBE DETAIL`` remains
the authoritative source for managed Delta properties and clustering; and
documented information_schema relations supply keys and tags. There is no
fallback for catalogs without information_schema (e.g. hive_metastore).

The connector is never imported at runtime: the connection is duck-typed
(``.cursor()`` context manager with ``execute``/``fetchone``/``fetchall``),
so this backend imports nothing beyond the shared adapter core.

Complex-typed DESCRIBE DETAIL fields (the ``properties`` map,
``clusteringColumns``) arrive as JSON strings from the connector by default;
the shared detail-row mappers accept both that shape and native values, so
either connection mode reads correctly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from delta_engine.adapters.databricks.errors import (
    exception_message,
    exception_type_name,
    is_missing_table,
)
from delta_engine.adapters.databricks.read import observed_table_from_reads
from delta_engine.adapters.databricks.sql import (
    describe_detail_query,
    describe_json_query,
    parse_described_table,
)
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import (
    CatalogState,
    ReadFailed,
    TableAbsent,
    TablePresent,
)
from delta_engine.domain.model import QualifiedName

if TYPE_CHECKING:
    from databricks.sql.client import Connection


class WarehouseReader:
    """Catalog state reader backed by a Databricks SQL warehouse connection."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """
        Fetch the current state of a table: present, absent, or unreadable.

        Every exception raised while reading — such as a connection error, an
        unsupported partition-column type, or a mid-read query failure —
        becomes a ``ReadFailed`` for this table rather than aborting the whole
        sync. Unmappable non-partition columns are skipped by the mapper and do
        not raise. The ``CatalogStateReader`` contract promises a
        ``CatalogState``, so the boundary must be total.
        """
        try:
            return self._read(qualified_name)
        except Exception as exception:
            return ReadFailed(
                failure=ReadFailure(exception_type_name(exception), exception_message(exception))
            )

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        """Read current state, letting any failure propagate to ``fetch_state``."""
        with self._connection.cursor() as cursor:
            try:
                cursor.execute(describe_json_query(qualified_name))
            except Exception as exception:
                if is_missing_table(exception):
                    return TableAbsent()
                raise
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"DESCRIBE TABLE AS JSON returned no row for {qualified_name}")
            described = parse_described_table(row[0], qualified_name)
            observed = observed_table_from_reads(
                qualified_name,
                columns=described.columns,
                comment=described.comment,
                detail_row=_describe_detail_row(cursor, qualified_name),
                partitioned_by=described.partitioned_by,
                run_info_schema_query=lambda query: _fetch_all(cursor, query),
            )
        return TablePresent(table=observed)


def _fetch_all(cursor: Any, query: str) -> list[Any]:
    """Run one query on the cursor and return all rows."""
    cursor.execute(query)
    return cursor.fetchall()


def _describe_detail_row(cursor: Any, qualified_name: QualifiedName) -> Any:
    """
    Return the table's DESCRIBE DETAIL row.

    Raises when the query yields no row for a table just described as present:
    an empty result there is not "a table with no
    properties" (that is a present row with an empty map) but a race or a
    catalog inconsistency. Failing loud lets ``fetch_state``'s error
    boundary return ``ReadFailed`` — the honest outcome for "could not
    determine state" — rather than a ``TablePresent`` with no properties,
    which would make the differ re-apply every managed property on every
    sync.
    """
    rows = _fetch_all(cursor, describe_detail_query(qualified_name))
    if not rows:
        raise RuntimeError(
            f"DESCRIBE DETAIL returned no rows for {qualified_name}, which"
            " DESCRIBE TABLE just reported as present — the table was"
            " dropped mid-read or the catalog is inconsistent."
        )
    return rows[0]
