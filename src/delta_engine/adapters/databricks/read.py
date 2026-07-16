"""
Shared catalog-state read for the Databricks backends.

Both backends read a table the same way: one ``DESCRIBE TABLE EXTENDED … AS
JSON`` for the columns and layout, then the constraint and tag metadata read
from information_schema as structured rows — Unity Catalog tags, this table's
own primary and foreign keys, and inbound foreign keys. Only how a query is
physically run differs per backend, so it is injected as a callable:
``read_catalog_state`` is the one entry point both readers call, the
read-side twin of the write side's ``execution.execute_statements``. Each
backend supplies only how a query runs (returning its rows) and owns its own
connection resource; the describe, parsing, assembly, and the total failure
boundary all live here. This module stays PySpark-free.
"""

from dataclasses import replace
from types import MappingProxyType

from delta_engine.adapters.databricks.errors import (
    exception_message,
    exception_type_name,
    is_missing_relation,
)
from delta_engine.adapters.databricks.sql import (
    RunQuery,
    describe_json_query,
    read_column_tags,
    read_foreign_keys,
    read_primary_key,
    read_referencing_foreign_keys,
    read_table_tags,
)
from delta_engine.adapters.databricks.sql.describe import (
    TableDescription,
    table_description_from_rows,
)
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import ObservedTable, QualifiedName


def read_catalog_state(run_query: RunQuery, qualified_name: QualifiedName) -> CatalogState:
    """
    Read one table's catalog state: ``TablePresent`` | ``TableAbsent`` | ``ReadFailed``.

    ``run_query`` runs one SQL statement and returns its rows; the same callable
    serves the AS JSON describe and the information_schema follow-ups. Every
    backend failure is caught and rendered here, so the port stays total for
    whatever a backend raises — including a connection that cannot run the
    first query.
    """
    try:
        description = _describe_table(run_query, qualified_name)
        if description is None:
            return TableAbsent()
        return TablePresent(table=_observed_table(run_query, description))
    except Exception as exception:
        return ReadFailed(
            failure=ReadFailure(exception_type_name(exception), exception_message(exception))
        )


def _describe_table(
    run_query: RunQuery,
    qualified_name: QualifiedName,
) -> TableDescription | None:
    """Describe the table: the parsed description, or ``None`` when it does not exist."""
    try:
        rows = run_query(describe_json_query(qualified_name))
    except Exception as exception:
        if is_missing_relation(exception):
            return None
        raise
    return table_description_from_rows(rows, qualified_name)


def _observed_table(run_query: RunQuery, description: TableDescription) -> ObservedTable:
    """Attach the information_schema metadata (tags, keys, inbound FKs) to the description."""
    qualified_name = description.qualified_name
    column_tags = read_column_tags(run_query, qualified_name)
    tagged_columns = tuple(
        replace(column, tags=column_tags.get(column.name, MappingProxyType({})))
        for column in description.columns
    )
    return ObservedTable(
        qualified_name=qualified_name,
        columns=tagged_columns,
        comment=description.comment,
        properties=description.properties,
        tags=read_table_tags(run_query, qualified_name),
        partitioned_by=description.partitioned_by,
        clustered_by=description.clustered_by,
        primary_key=read_primary_key(run_query, qualified_name),
        foreign_keys=read_foreign_keys(run_query, qualified_name),
        referencing_foreign_keys=read_referencing_foreign_keys(run_query, qualified_name),
    )
