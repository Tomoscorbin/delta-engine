"""
Shared catalog-state read for the Databricks backends.

Both backends read a table the same way: ``DESCRIBE TABLE EXTENDED … AS JSON``
for columns, layout, properties, and protocol features, then the constraint
and tag metadata from information_schema. Only how a query is physically run
differs per backend, so it is injected as a callable:
``read_catalog_state`` is the one entry point both readers call, the
read-side twin of the write side's ``execution.execute_statement``. Each
backend supplies only how a query runs (returning its rows) and owns its own
connection resource; the describe, the acceptance policies (which relation
kinds are read, which observed property keys become engine state), assembly,
and the typed error boundary all live here. This module stays PySpark-free.
"""

from collections.abc import Mapping
from dataclasses import replace
import logging
from types import MappingProxyType
from typing import Final

from delta_engine.adapters.databricks.exception_inspection import (
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
from delta_engine.adapters.databricks.table_features import (
    supported_features_from_properties,
)
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import CatalogState, TableAbsent, TablePresent
from delta_engine.application.properties import DELTA_PROPERTY_POLICY
from delta_engine.domain.model import ObservedTable, QualifiedName, TableKind


class UnsupportedRelationError(Exception):
    """The described relation is not a kind of table the engine manages."""


logger = logging.getLogger(__name__)


# The engine reads and reconciles the Delta relations it can ALTER: ordinary
# tables, managed or external (existing external tables can be altered;
# creating one is not yet supported — CREATE TABLE emits no LOCATION), and
# streaming tables, which take the ALTER STREAMING TABLE dialect and admit
# tag changes only — the reader states the kind; validation's eligibility
# checks enforce the restriction. Anything else a catalog name can resolve to — a
# view, a materialized view, a foreign table, a non-Delta format — cannot be
# represented as engine state, so the read admits exactly these kinds and
# fails closed on everything else, including kinds Databricks adds in the
# future.
_TABLE_KINDS_BY_RELATION_TYPE: Final[Mapping[str, TableKind]] = MappingProxyType(
    {
        "MANAGED": TableKind.TABLE,
        "EXTERNAL": TableKind.TABLE,
        "STREAMING_TABLE": TableKind.STREAMING_TABLE,
    }
)
_SUPPORTED_PROVIDERS: Final = {"delta"}


def read_catalog_state(run_query: RunQuery, qualified_name: QualifiedName) -> CatalogState:
    """
    Read one table's catalog state: ``TablePresent`` or ``TableAbsent``.

    ``run_query`` runs one SQL statement and returns its rows; the same callable
    serves the AS JSON describe and the information_schema follow-ups. Every
    backend failure is caught and translated here, including a connection that
    cannot run the first query.

    Raises:
        ReadError: The catalog state could not be determined.

    """
    try:
        description = _describe_table(run_query, qualified_name)
        if description is None:
            return TableAbsent()
        kind = _supported_relation_kind(description)
        return TablePresent(table=_read_observed_table(run_query, description, kind))
    except Exception as exception:
        message = exception_message(exception)
        logger.warning("Read failed for %s: %s", qualified_name, message, exc_info=True)
        raise ReadError(
            exception_type=exception_type_name(exception),
            message=message,
        ) from exception


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


def _supported_relation_kind(description: TableDescription) -> TableKind:
    """
    Map the described relation onto the kind the engine manages it as.

    Relations in the admit mapping with a supported provider map onto their
    kind. Any other relation — a view, a materialized view, a foreign table,
    a non-Delta format, or an unknown future kind — raises rather than being
    modelled as a table. The error names the admitted set from the mapping
    itself, so it cannot go stale against it.
    """
    kind = _TABLE_KINDS_BY_RELATION_TYPE.get(description.relation_type or "")
    if kind is not None and description.provider in _SUPPORTED_PROVIDERS:
        return kind
    supported_types = ", ".join(sorted(_TABLE_KINDS_BY_RELATION_TYPE))
    supported_providers = ", ".join(sorted(_SUPPORTED_PROVIDERS))
    raise UnsupportedRelationError(
        f"{description.qualified_name}: the engine reads relations of type"
        f" {supported_types} with provider {supported_providers}; this relation"
        f" has type={description.relation_type!r}, provider={description.provider!r}"
    )


def _read_observed_table(
    run_query: RunQuery, description: TableDescription, kind: TableKind
) -> ObservedTable:
    """Read the information_schema metadata (tags, keys, inbound FKs) and assemble the table."""
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
        properties=DELTA_PROPERTY_POLICY.project_observed(description.table_properties),
        supported_features=supported_features_from_properties(description.table_properties),
        tags=read_table_tags(run_query, qualified_name),
        partitioned_by=description.partitioned_by,
        clustered_by=description.clustered_by,
        primary_key=read_primary_key(run_query, qualified_name),
        foreign_keys=read_foreign_keys(run_query, qualified_name),
        referencing_foreign_keys=read_referencing_foreign_keys(run_query, qualified_name),
        kind=kind,
    )
