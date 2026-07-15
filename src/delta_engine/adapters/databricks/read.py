"""
Shared catalog-state read for the Databricks backends.

Both backends read a table the same way: one ``DESCRIBE TABLE EXTENDED … AS
JSON`` parsed into a backend-neutral ``TableSnapshot``, then the metadata the
JSON omits — Unity Catalog tags and inbound foreign keys — attached from
information_schema. Only how a query is physically run differs per backend, so
it is injected as a callable: ``read_catalog_state`` is the read-side twin of
the write side's ``execution.execute_statements``. Each backend supplies only
how a query runs (returning its rows) and owns its own connection resource;
the describe, parsing, assembly, and the total failure boundary all live here.
This module stays PySpark-free.
"""

from collections.abc import Callable, Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from delta_engine.adapters.databricks.errors import (
    exception_message,
    exception_type_name,
    is_missing_relation,
)
from delta_engine.adapters.databricks.sql import (
    column_tags_from_rows,
    column_tags_query,
    describe_json_query,
    referencing_foreign_keys_from_rows,
    referencing_foreign_keys_query,
    table_tags_from_rows,
    table_tags_query,
)
from delta_engine.adapters.databricks.sql.describe_json import TableSnapshot, parse_table_snapshot
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import ObservedTable, QualifiedName


def read_catalog_state(
    run_query: Callable[[str], Sequence[Any]],
    qualified_name: QualifiedName,
) -> CatalogState:
    """
    Read one table's catalog state: ``TablePresent`` | ``TableAbsent`` | ``ReadFailed``.

    ``run_query`` runs one SQL statement and returns its rows; the same callable
    serves the AS JSON describe and the information_schema follow-ups. Every
    backend failure is caught and rendered here, so the port stays total for
    whatever a backend raises — including a connection that cannot run the
    first query.
    """
    try:
        return _read(run_query, qualified_name)
    except Exception as exception:
        return ReadFailed(
            failure=ReadFailure(exception_type_name(exception), exception_message(exception))
        )


def _read(run_query: Callable[[str], Sequence[Any]], qualified_name: QualifiedName) -> CatalogState:
    try:
        rows = run_query(describe_json_query(qualified_name))
    except Exception as exception:
        if is_missing_relation(exception):
            return TableAbsent()
        raise
    if not rows:
        raise RuntimeError(f"DESCRIBE AS JSON returned no row for {qualified_name}")
    snapshot = parse_table_snapshot(rows[0][0], qualified_name)
    observed = observed_table_from_snapshot(snapshot, run_info_schema_query=run_query)
    return TablePresent(table=observed)


def observed_table_from_snapshot(
    snapshot: TableSnapshot,
    *,
    run_info_schema_query: Callable[[str], Sequence[Any]],
) -> ObservedTable:
    """Assemble the domain ``ObservedTable`` from a snapshot plus information_schema."""
    qualified_name = snapshot.qualified_name
    column_tags = column_tags_from_rows(run_info_schema_query(column_tags_query(qualified_name)))
    tagged_columns = tuple(
        replace(column, tags=column_tags.get(column.name, MappingProxyType({})))
        for column in snapshot.columns
    )
    table_tags = table_tags_from_rows(run_info_schema_query(table_tags_query(qualified_name)))
    referencing_foreign_keys = referencing_foreign_keys_from_rows(
        run_info_schema_query(referencing_foreign_keys_query(qualified_name))
    )
    return ObservedTable(
        qualified_name=qualified_name,
        columns=tagged_columns,
        comment=snapshot.comment,
        properties=snapshot.properties,
        tags=table_tags,
        partitioned_by=snapshot.partitioned_by,
        clustered_by=snapshot.clustered_by,
        primary_key=snapshot.primary_key,
        foreign_keys=snapshot.foreign_keys,
        referencing_foreign_keys=referencing_foreign_keys,
    )
