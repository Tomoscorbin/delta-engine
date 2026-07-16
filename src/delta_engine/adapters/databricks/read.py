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
connection resource; the describe, the relation acceptance policy, assembly,
and the total failure boundary all live here. This module stays PySpark-free.
"""

from dataclasses import replace
from types import MappingProxyType
from typing import Final

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
    schema_exists,
)
from delta_engine.adapters.databricks.sql.describe import (
    TableDescription,
    table_description_from_rows,
)
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import ObservedTable, QualifiedName


class UnsupportedRelationError(Exception):
    """The described relation is not a kind of table the engine manages."""


# The engine reads and reconciles ordinary Delta tables, managed or external
# (existing external tables can be altered; creating one is not yet supported —
# CREATE TABLE emits no LOCATION). Anything else a catalog name can resolve to
# — a view, a materialized view, a streaming table, a foreign table, a
# non-Delta format — cannot be represented as engine state, so the read admits
# exactly these kinds and fails closed on everything else, including kinds
# Databricks adds in the future.
_SUPPORTED_RELATION_TYPES: Final = {"MANAGED", "EXTERNAL"}
_SUPPORTED_PROVIDERS: Final = {"delta"}


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
        _require_supported_relation(description)
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
        if not is_missing_relation(exception):
            raise
        # The describe reports TABLE_OR_VIEW_NOT_FOUND even when the schema or
        # the catalog is what is missing (verified live), so absence needs
        # positive confirmation. The engine creates tables, never their
        # containers: reading a missing container as "absent" would plan a
        # CREATE TABLE that cannot succeed — and a dry run would report that
        # impossible plan as success.
        if not schema_exists(run_query, qualified_name):
            raise RuntimeError(
                f"schema {qualified_name.catalog}.{qualified_name.schema} does not exist"
            ) from exception
        return None
    return table_description_from_rows(rows, qualified_name)


def _require_supported_relation(description: TableDescription) -> None:
    """
    Check the described relation is a kind of table the engine manages.

    The engine reads managed and external Delta tables. Any other relation —
    a view, a streaming table, a foreign table, a non-Delta format, or an
    unknown future kind — raises rather than being modelled as a table.
    """
    if (
        description.relation_type in _SUPPORTED_RELATION_TYPES
        and description.provider in _SUPPORTED_PROVIDERS
    ):
        return
    raise UnsupportedRelationError(
        f"{description.qualified_name}: the engine manages MANAGED or EXTERNAL Delta tables; "
        f"this relation has type={description.relation_type!r}, "
        f"provider={description.provider!r}"
    )


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
