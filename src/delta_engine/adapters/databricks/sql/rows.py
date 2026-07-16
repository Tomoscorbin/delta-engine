"""
Read information_schema metadata into domain values.

One function per aspect: each renders its query (:mod:`queries`), runs it
through the injected ``run_query``, and maps the rows to a domain value — the
SQL text, the row shape, and the mapping stay behind one name. Rows are
duck-typed catalog rows — pyspark ``Row`` or databricks-sql ``Row`` —
accessed only by attribute lookups, so this module stays PySpark-free like
the rest of the package.

Identifier fields (constraint, column, table names) are casefolded here: the
domain model requires lowercase identifiers, and normalising at the adapter
boundary keeps that impedance mismatch out of the domain. Tag keys and values
are case-sensitive and preserved verbatim.
"""

from collections.abc import Callable, Sequence
from types import MappingProxyType
from typing import Any

from delta_engine.adapters.databricks.sql.queries import (
    column_tags_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
    QualifiedName,
)

# Duck-typed catalog rows, as a backend query returns them.
type Rows = Sequence[Any]
# Runs one SQL statement and returns its rows — the one fact a backend supplies.
type RunQuery = Callable[[str], Rows]


def read_primary_key(
    run_query: RunQuery,
    qualified_name: QualifiedName,
) -> PrimaryKeyConstraint | None:
    """
    Read this table's primary key, or ``None`` when it has none.

    Rows arrive one per key column, ordered by the column's position in the
    key; a table has at most one primary key, so they all share its name.
    """
    ordered = list(run_query(primary_key_query(qualified_name)))
    if not ordered:
        return None
    return PrimaryKeyConstraint(
        columns=tuple(row.column_name.casefold() for row in ordered),
        constraint_name=ordered[0].constraint_name.casefold(),
    )


def read_foreign_keys(
    run_query: RunQuery,
    qualified_name: QualifiedName,
) -> tuple[ForeignKeyConstraint, ...]:
    """
    Read the foreign keys this table owns.

    Rows arrive one per foreign-key column, grouped by constraint and ordered so
    that each key's local and referenced columns align positionally. Every
    column of one key shares a single referenced table.
    """
    grouped: dict[str, list[Any]] = {}
    for row in run_query(foreign_keys_query(qualified_name)):
        grouped.setdefault(row.constraint_name.casefold(), []).append(row)
    return tuple(
        ForeignKeyConstraint(
            local_columns=tuple(row.local_column.casefold() for row in group),
            referenced_table=QualifiedName(
                group[0].referenced_catalog.casefold(),
                group[0].referenced_schema.casefold(),
                group[0].referenced_table.casefold(),
            ),
            referenced_columns=tuple(row.referenced_column.casefold() for row in group),
            constraint_name=constraint_name,
        )
        for constraint_name, group in grouped.items()
    )


def read_referencing_foreign_keys(
    run_query: RunQuery,
    qualified_name: QualifiedName,
) -> tuple[ForeignKeyReference, ...]:
    """Read the inbound foreign keys owned by other tables that reference this one."""
    return tuple(
        ForeignKeyReference(
            constraint_name=row.constraint_name.casefold(),
            referencing_table=QualifiedName(
                row.referencing_catalog.casefold(),
                row.referencing_schema.casefold(),
                row.referencing_table.casefold(),
            ),
        )
        for row in run_query(referencing_foreign_keys_query(qualified_name))
    )


def read_table_tags(
    run_query: RunQuery,
    qualified_name: QualifiedName,
) -> MappingProxyType[str, str]:
    """Read this table's Unity Catalog tags; tag case is preserved verbatim."""
    return MappingProxyType(
        {row.tag_name: row.tag_value for row in run_query(table_tags_query(qualified_name))}
    )


def read_column_tags(
    run_query: RunQuery,
    qualified_name: QualifiedName,
) -> MappingProxyType[str, MappingProxyType[str, str]]:
    """
    Read all column tags of this table as ``{column_name: {tag: value}}``.

    Column names are casefolded to match the domain's lowercase columns; tag
    keys and values are case-sensitive and returned verbatim.
    """
    grouped: dict[str, dict[str, str]] = {}
    for row in run_query(column_tags_query(qualified_name)):
        grouped.setdefault(row.column_name.casefold(), {})[row.tag_name] = row.tag_value
    return MappingProxyType({column: MappingProxyType(tags) for column, tags in grouped.items()})
