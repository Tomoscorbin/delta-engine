"""
Turn information_schema rows into domain values.

The row-mapping counterpart of :mod:`queries` for the supplementary reads:
each tag / inbound-foreign-key query builder there has its mapper here, so the
row shape a query produces and the domain value it becomes are defined in one
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

from delta_engine.domain.model import ForeignKeyReference, QualifiedName


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
