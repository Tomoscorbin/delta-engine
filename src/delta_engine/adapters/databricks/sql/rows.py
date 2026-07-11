"""
Map information_schema rows to domain values.

The row-mapping counterpart of :mod:`queries`: each query builder there has
its mapper here, so the row shape a query produces and the domain value it
becomes are defined in one shared place. Rows are duck-typed catalog rows —
pyspark ``Row`` or databricks-sql ``Row`` — accessed only by attribute and
``row["name"]`` item lookups, which both support; this module stays
PySpark-free like the rest of the package.

Identifier fields (constraint, column, table names) are casefolded here:
the domain model requires lowercase identifiers, and normalising at the
adapter boundary keeps that impedance mismatch out of the domain. Tag keys
and values are case-sensitive and preserved verbatim.
"""

from collections.abc import Iterable, Mapping, Sequence
from itertools import groupby
from types import MappingProxyType
from typing import Any

from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
    QualifiedName,
)


def primary_key_from_rows(rows: Sequence[Any]) -> PrimaryKeyConstraint | None:
    """
    Build the primary key from its information_schema rows, or ``None``.

    The query orders rows by ordinal_position, so the columns tuple is in
    key order.
    """
    columns = tuple(row["column_name"].casefold() for row in rows)
    if not columns:
        return None
    constraint_name = rows[0]["constraint_name"].casefold()
    return PrimaryKeyConstraint(columns=columns, constraint_name=constraint_name)


def foreign_keys_from_rows(rows: Iterable[Any]) -> tuple[ForeignKeyConstraint, ...]:
    """
    Build all foreign keys from information_schema rows.

    The query orders by (constraint_name, ordinal_position), so each
    constraint's rows are contiguous and already in column order. groupby
    yields one contiguous run per constraint without a manual accumulator.
    """
    return tuple(
        _foreign_key_from_rows(constraint_name, list(constraint_rows))
        for constraint_name, constraint_rows in groupby(rows, key=lambda row: row.constraint_name)
    )


def _foreign_key_from_rows(constraint_name: str, rows: list[Any]) -> ForeignKeyConstraint:
    """
    Build one foreign key constraint from its key_column_usage rows.

    ``rows`` are all rows for a single constraint, ordered by
    ordinal_position, so local and referenced columns stay positionally
    aligned. The referenced table is identical on every row, so it is read
    from the first. Constraint names are read from the catalog so
    observed-only constraints can be dropped by their real names.
    """
    first = rows[0]
    return ForeignKeyConstraint(
        local_columns=tuple(row.local_column.casefold() for row in rows),
        referenced_table=QualifiedName(
            first.ref_catalog.casefold(),
            first.ref_schema.casefold(),
            first.ref_table.casefold(),
        ),
        referenced_columns=tuple(row.ref_column.casefold() for row in rows),
        constraint_name=constraint_name.casefold(),
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


def managed_properties_from_mapping(properties: Mapping[str, str]) -> MappingProxyType[str, str]:
    """
    Filter observed table properties to the managed registry keys.

    Platform-written keys (protocol bookkeeping, auto-enabled features,
    internal counters) never reach the domain, so they can neither trip
    validation nor churn plans. This is backend normalization, owned at the
    adapter boundary like identifier lowercasing and type parsing.
    """
    managed = {name: value for name, value in properties.items() if name in DELTA_PROPERTY_REGISTRY}
    return MappingProxyType(managed)
