"""
Shared observed-table assembly for the Databricks backends.

Both backends build the same ``ObservedTable`` from the same information_schema
queries and DESCRIBE DETAIL, feeding each query's rows to the same shared row
mapper. Only one fact differs per backend — how information_schema rows are
physically fetched — so it is injected as a callable, the read-side twin of the
runner ``execution.execute_statements`` injects on the write side.

The backend reads the facts whose *source* genuinely differs (existence, the
columns, the table comment, the DESCRIBE DETAIL row, and the partition order)
and passes them in; this module owns the query-to-mapper-to-field wiring that
would otherwise be duplicated across the two readers and drift apart. It stays
PySpark-free: the injected runner does the I/O, so importing it never pulls a
backend.
"""

from collections.abc import Callable, Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from delta_engine.adapters.databricks.sql import (
    clustering_columns_from_detail_row,
    column_tags_from_rows,
    column_tags_query,
    foreign_keys_from_rows,
    foreign_keys_query,
    managed_properties_from_detail_row,
    primary_key_from_rows,
    primary_key_query,
    referencing_foreign_keys_from_rows,
    referencing_foreign_keys_query,
    table_tags_from_rows,
    table_tags_query,
)
from delta_engine.domain.model import ObservedColumn, ObservedTable, QualifiedName


def observed_table_from_reads(
    qualified_name: QualifiedName,
    *,
    columns: tuple[ObservedColumn, ...],
    comment: str,
    detail_row: Any,
    partitioned_by: tuple[str, ...],
    run_info_schema_query: Callable[[str], Sequence[Any]],
) -> ObservedTable:
    """
    Assemble an ``ObservedTable`` from a backend's reads plus information_schema.

    ``columns`` are the table's observed columns *without* tags; this function
    attaches column tags read from information_schema. ``run_info_schema_query``
    runs one information_schema query and returns its rows — the Spark reader
    passes its availability-gated reader, the warehouse reader a cursor fetch —
    so the two backends share this assembly while differing only in how a query
    is executed.
    """
    column_tags = column_tags_from_rows(run_info_schema_query(column_tags_query(qualified_name)))
    tagged_columns = tuple(
        replace(column, tags=column_tags.get(column.name, MappingProxyType({})))
        for column in columns
    )
    return ObservedTable(
        qualified_name=qualified_name,
        columns=tagged_columns,
        comment=comment,
        properties=managed_properties_from_detail_row(detail_row),
        tags=table_tags_from_rows(run_info_schema_query(table_tags_query(qualified_name))),
        partitioned_by=partitioned_by,
        clustered_by=clustering_columns_from_detail_row(detail_row),
        primary_key=primary_key_from_rows(run_info_schema_query(primary_key_query(qualified_name))),
        foreign_keys=foreign_keys_from_rows(
            run_info_schema_query(foreign_keys_query(qualified_name))
        ),
        referencing_foreign_keys=referencing_foreign_keys_from_rows(
            run_info_schema_query(referencing_foreign_keys_query(qualified_name))
        ),
    )
