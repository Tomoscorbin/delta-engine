"""
Reader adapter for Databricks SQL warehouses.

Unity Catalog only: every read comes from information_schema and
DESCRIBE DETAIL over a databricks-sql connection — there is no fallback for
catalogs without information_schema (e.g. hive_metastore); such reads
surface as ``ReadFailed`` with the backend's error. The connector is never
imported at runtime: the connection is duck-typed (``.cursor()`` context
manager with ``execute``/``fetchall``), so this backend imports nothing
beyond the shared adapter core.

Complex-typed DESCRIBE DETAIL fields (the ``properties`` map,
``clusteringColumns``) arrive as JSON strings from the connector by default;
the shared detail-row mappers accept both that shape and native values, so
either connection mode reads correctly.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from delta_engine.adapters.databricks.errors import exception_type_name
from delta_engine.adapters.databricks.sql import (
    clustering_columns_from_detail_row,
    column_from_catalog,
    column_tags_from_rows,
    column_tags_query,
    columns_query,
    describe_detail_query,
    foreign_keys_from_rows,
    foreign_keys_query,
    managed_properties_from_detail_row,
    primary_key_from_rows,
    primary_key_query,
    referencing_foreign_keys_from_rows,
    referencing_foreign_keys_query,
    table_row_query,
    table_tags_from_rows,
    table_tags_query,
)
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import (
    CatalogState,
    ReadFailed,
    TableAbsent,
    TablePresent,
)
from delta_engine.domain.model import (
    ObservedColumn,
    ObservedTable,
    QualifiedName,
)

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
            return ReadFailed(failure=ReadFailure(exception_type_name(exception), str(exception)))

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        """Read current state, letting any failure propagate to ``fetch_state``."""
        with self._connection.cursor() as cursor:
            table_rows = _fetch_all(cursor, table_row_query(qualified_name))
            if not table_rows:
                return TableAbsent()
            comment = table_rows[0].comment or ""

            column_rows = _fetch_all(cursor, columns_query(qualified_name))
            column_tags = column_tags_from_rows(
                _fetch_all(cursor, column_tags_query(qualified_name))
            )
            columns = tuple(
                replace(column, tags=column_tags.get(column.name, MappingProxyType({})))
                for column in _to_columns(column_rows, qualified_name)
            )

            detail = _describe_detail_row(cursor, qualified_name)
            observed = ObservedTable(
                qualified_name=qualified_name,
                columns=columns,
                comment=comment,
                properties=managed_properties_from_detail_row(detail),
                tags=table_tags_from_rows(_fetch_all(cursor, table_tags_query(qualified_name))),
                partitioned_by=_partitioned_by(column_rows),
                clustered_by=clustering_columns_from_detail_row(detail),
                primary_key=primary_key_from_rows(
                    _fetch_all(cursor, primary_key_query(qualified_name))
                ),
                foreign_keys=foreign_keys_from_rows(
                    _fetch_all(cursor, foreign_keys_query(qualified_name))
                ),
                referencing_foreign_keys=referencing_foreign_keys_from_rows(
                    _fetch_all(cursor, referencing_foreign_keys_query(qualified_name))
                ),
            )
        return TablePresent(table=observed)


def _fetch_all(cursor: Any, query: str) -> list[Any]:
    """Run one query on the cursor and return all rows."""
    cursor.execute(query)
    return cursor.fetchall()


def _describe_detail_row(cursor: Any, qualified_name: QualifiedName) -> Any:
    """
    Return the table's DESCRIBE DETAIL row.

    Raises when the query yields no row for a table information_schema just
    reported present: an empty result there is not "a table with no
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
            " information_schema just reported as present — the table was"
            " dropped mid-read or the catalog is inconsistent."
        )
    return rows[0]


def _to_columns(
    column_rows: Sequence[Any], qualified_name: QualifiedName
) -> tuple[ObservedColumn, ...]:
    """Map information_schema.columns rows to observed columns, skipping unmappable types."""
    columns = []
    for row in column_rows:
        column = column_from_catalog(
            name=row.column_name,
            type_text=row.full_data_type,
            nullable=row.is_nullable == "YES",
            comment=row.comment or "",
            is_partition=row.partition_index is not None,
            qualified_name=qualified_name,
        )
        if column is not None:
            columns.append(column)
    return tuple(columns)


def _partitioned_by(column_rows: Sequence[Any]) -> tuple[str, ...]:
    """Partition column names in ascending catalog ``partition_index`` order."""
    partition_rows = sorted(
        (row for row in column_rows if row.partition_index is not None),
        key=lambda row: row.partition_index,
    )
    return tuple(row.column_name.casefold() for row in partition_rows)
