"""
Turn information_schema rows into domain values.

The row-mapping counterpart of :mod:`queries` for the supplementary reads:
each constraint / tag query builder there has its mapper here, so the row
shape a query produces and the domain value it becomes are defined in one
shared place. Rows are duck-typed catalog rows — pyspark ``Row`` or
databricks-sql ``Row`` — accessed only by attribute lookups, so this module
stays PySpark-free like the rest of the package.

Identifier fields (constraint, column, table names) are casefolded here: the
domain model requires lowercase identifiers, and normalising at the adapter
boundary keeps that impedance mismatch out of the domain. Tag keys and values
are case-sensitive and preserved verbatim.
"""

from collections.abc import Iterable
from types import MappingProxyType
from typing import Any

from delta_engine.domain.model import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
    QualifiedName,
)


def primary_key_from_rows(rows: Iterable[Any]) -> PrimaryKeyConstraint | None:
    """
    Build this table's primary key from information_schema rows, or ``None``.

    Rows arrive one per key column, ordered by the column's position in the
    key; a table has at most one primary key, so they all share its name. No
    rows means the table has no primary key.
    """
    ordered = list(rows)
    if not ordered:
        return None
    return PrimaryKeyConstraint(
        columns=tuple(row.column_name.casefold() for row in ordered),
        constraint_name=ordered[0].constraint_name.casefold(),
    )


def foreign_keys_from_rows(rows: Iterable[Any]) -> tuple[ForeignKeyConstraint, ...]:
    """
    Build this table's foreign keys from information_schema rows.

    Rows arrive one per foreign-key column, grouped by constraint and ordered so
    that each key's local and referenced columns align positionally. Every
    column of one key shares a single referenced table.
    """
    grouped: dict[str, list[Any]] = {}
    for row in rows:
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


def referencing_foreign_keys_from_rows(rows: Iterable[Any]) -> tuple[ForeignKeyReference, ...]:
    """Build the inbound foreign key references from information_schema rows."""
    return tuple(
        ForeignKeyReference(
            constraint_name=row.constraint_name.casefold(),
            referencing_table=QualifiedName(
                row.referencing_catalog.casefold(),
                row.referencing_schema.casefold(),
                row.referencing_table.casefold(),
            ),
        )
        for row in rows
    )


def table_tags_from_rows(rows: Iterable[Any]) -> MappingProxyType[str, str]:
    """Map table-tag rows to a read-only mapping; tag case is preserved verbatim."""
    return MappingProxyType({row.tag_name: row.tag_value for row in rows})


def column_tags_from_rows(
    rows: Iterable[Any],
) -> MappingProxyType[str, MappingProxyType[str, str]]:
    """
    Map column-tag rows to ``{column_name: {tag: value}}``.

    Column names are casefolded to match the domain's lowercase columns; tag
    keys and values are case-sensitive and returned verbatim.
    """
    grouped: dict[str, dict[str, str]] = {}
    for row in rows:
        grouped.setdefault(row.column_name.casefold(), {})[row.tag_name] = row.tag_value
    return MappingProxyType({column: MappingProxyType(tags) for column, tags in grouped.items()})
