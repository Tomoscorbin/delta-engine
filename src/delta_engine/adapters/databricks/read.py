"""
Shared catalog-state read for the Databricks backends.

Both backends read a table the same way: one ``DESCRIBE TABLE EXTENDED … AS
JSON`` parsed into a backend-neutral ``TableDescription`` for the columns and
layout, then the constraint and tag metadata read from information_schema as
structured rows — Unity Catalog tags, this table's own primary and foreign
keys, and inbound foreign keys. Only how a query is physically run differs per
backend, so it is injected as a callable: ``read_catalog_state`` is the
read-side twin of the write side's ``execution.execute_statements``. Each
backend supplies only how a query runs (returning its rows) and owns its own
connection resource; the describe, parsing, assembly, and the total failure
boundary all live here. This module stays PySpark-free.
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
    foreign_keys_from_rows,
    foreign_keys_query,
    primary_key_from_rows,
    primary_key_query,
    referencing_foreign_keys_from_rows,
    referencing_foreign_keys_query,
    schema_exists_query,
    table_tags_from_rows,
    table_tags_query,
)
from delta_engine.adapters.databricks.sql.describe import TableDescription, parse_table_description
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
        if not is_missing_relation(exception):
            raise
        # The describe reports TABLE_OR_VIEW_NOT_FOUND even when the schema or
        # the catalog is what is missing (verified live), so absence needs
        # positive confirmation. The engine creates tables, never their
        # containers: reading a missing container as "absent" would plan a
        # CREATE TABLE that cannot succeed — and a dry run would report that
        # impossible plan as success.
        if not _schema_exists(run_query, qualified_name):
            raise RuntimeError(
                f"schema {qualified_name.catalog}.{qualified_name.schema} does not exist"
            ) from exception
        return TableAbsent()
    if not rows:
        raise RuntimeError(f"DESCRIBE AS JSON returned no row for {qualified_name}")
    description = parse_table_description(rows[0][0], qualified_name)
    observed = observed_table_from_description(description, run_info_schema_query=run_query)
    return TablePresent(table=observed)


def _schema_exists(
    run_query: Callable[[str], Sequence[Any]],
    qualified_name: QualifiedName,
) -> bool:
    """Whether the table's parent schema exists; raises when the catalog cannot be read."""
    return bool(run_query(schema_exists_query(qualified_name)))


def observed_table_from_description(
    description: TableDescription,
    *,
    run_info_schema_query: Callable[[str], Sequence[Any]],
) -> ObservedTable:
    """Assemble the domain ``ObservedTable`` from a description plus information_schema."""
    qualified_name = description.qualified_name
    column_tags = column_tags_from_rows(run_info_schema_query(column_tags_query(qualified_name)))
    tagged_columns = tuple(
        replace(column, tags=column_tags.get(column.name, MappingProxyType({})))
        for column in description.columns
    )
    table_tags = table_tags_from_rows(run_info_schema_query(table_tags_query(qualified_name)))
    primary_key = primary_key_from_rows(run_info_schema_query(primary_key_query(qualified_name)))
    foreign_keys = foreign_keys_from_rows(run_info_schema_query(foreign_keys_query(qualified_name)))
    referencing_foreign_keys = referencing_foreign_keys_from_rows(
        run_info_schema_query(referencing_foreign_keys_query(qualified_name))
    )
    return ObservedTable(
        qualified_name=qualified_name,
        columns=tagged_columns,
        comment=description.comment,
        properties=description.properties,
        tags=table_tags,
        partitioned_by=description.partitioned_by,
        clustered_by=description.clustered_by,
        primary_key=primary_key,
        foreign_keys=foreign_keys,
        referencing_foreign_keys=referencing_foreign_keys,
    )
