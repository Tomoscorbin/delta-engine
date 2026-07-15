"""
Shared observed-table assembly for the Databricks backends.

Both backends parse one AS JSON document into a ``TableSnapshot`` (table-local
state) and then attach the metadata that is not in the JSON — Unity Catalog
tags and inbound foreign keys — read through information_schema. Only how a
query is physically run differs per backend, so it is injected as a callable:
the read-side twin of the runner ``execution.execute_statements`` injects on
the write side. This module stays PySpark-free.
"""

from collections.abc import Callable, Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from delta_engine.adapters.databricks.sql import (
    column_tags_from_rows,
    column_tags_query,
    referencing_foreign_keys_from_rows,
    referencing_foreign_keys_query,
    table_tags_from_rows,
    table_tags_query,
)
from delta_engine.adapters.databricks.sql.describe_json import TableSnapshot
from delta_engine.domain.model import ObservedTable


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
    return ObservedTable(
        qualified_name=qualified_name,
        columns=tagged_columns,
        comment=snapshot.comment,
        properties=snapshot.properties,
        tags=table_tags_from_rows(run_info_schema_query(table_tags_query(qualified_name))),
        partitioned_by=snapshot.partitioned_by,
        clustered_by=snapshot.clustered_by,
        primary_key=snapshot.primary_key,
        foreign_keys=snapshot.foreign_keys,
        referencing_foreign_keys=referencing_foreign_keys_from_rows(
            run_info_schema_query(referencing_foreign_keys_query(qualified_name))
        ),
    )
