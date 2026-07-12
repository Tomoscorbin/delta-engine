"""
Turn catalog-reported facts into domain values.

The row-mapping counterpart of :mod:`queries`, covering information_schema
queries and DESCRIBE DETAIL: each query builder there has
its mapper here, so the row shape a query produces and the domain value it
becomes are defined in one shared place. The module also owns boundary
policy that is not tied to one row shape — ``column_from_catalog`` takes
the catalog's facts as keywords, because the Spark backend reads them from
catalog objects rather than query rows. Rows are duck-typed catalog rows —
pyspark ``Row`` or databricks-sql ``Row`` — accessed only by attribute
lookups and ``asDict()``, which both support; this module stays PySpark-free
like the rest of the package.

Identifier fields (constraint, column, table names) are casefolded here:
the domain model requires lowercase identifiers, and normalising at the
adapter boundary keeps that impedance mismatch out of the domain. Tag keys
and values are case-sensitive and preserved verbatim.
"""

from collections.abc import Iterable, Sequence
from itertools import groupby
import json
import logging
from types import MappingProxyType
from typing import Any

from delta_engine.adapters.databricks.sql.parse import parse_data_type
from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.domain.model import (
    Column,
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
    QualifiedName,
)

logger = logging.getLogger(__name__)


def column_from_catalog(
    *,
    name: str,
    type_text: str,
    nullable: bool,
    comment: str,
    is_partition: bool,
    qualified_name: QualifiedName,
) -> Column | None:
    """
    Build a domain ``Column`` from catalog-reported facts, or ``None``.

    Owns the shared unmappable-type policy for both read backends, so the
    two readers cannot drift apart on it. A column whose type string has no
    domain mapping returns ``None``, logging a warning so operators can
    track gaps as new types are released. A *partition* column with no
    domain mapping instead raises: skipping it would leave
    ``ObservedTable.partitioned_by`` silently incomplete, and the differ
    would fabricate a false ``PartitioningChanged`` from the gap. Raising
    lets ``fetch_state``'s exception boundary turn it into ``ReadFailed`` —
    the honest "could not determine state" — rather than a wrong diff.

    The name is casefolded here: the domain model requires lowercase
    identifiers, and case-preserving catalogs can report mixed-case names.
    """
    data_type = parse_data_type(type_text)
    if data_type is None:
        if is_partition:
            raise RuntimeError(
                f"Partition column {name!r} in {qualified_name} has"
                f" type {type_text!r}, which this version of"
                " delta-engine has no mapping for (catalogs gain new types"
                " before engines that pin a type model); the observed"
                " partitioning cannot be determined, so the table cannot be"
                " read safely."
            )
        logger.warning(
            "Skipping column %r in %s: unrecognised type %r"
            " — the column is invisible to this sync; if a declaration includes"
            " it, the planned ADD COLUMN will fail at execution because the"
            " column already exists",
            name,
            qualified_name,
            type_text,
        )
        return None

    return Column(
        name=name.casefold(),
        data_type=data_type,
        nullable=nullable,
        comment=comment,
    )


def primary_key_from_rows(rows: Sequence[Any]) -> PrimaryKeyConstraint | None:
    """
    Build the primary key from its information_schema rows, or ``None``.

    The query orders rows by ordinal_position, so the columns tuple is in
    key order.
    """
    columns = tuple(row.column_name.casefold() for row in rows)
    if not columns:
        return None
    constraint_name = rows[0].constraint_name.casefold()
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


def managed_properties_from_detail_row(row: Any) -> MappingProxyType[str, str]:
    """
    Return a DESCRIBE DETAIL row's properties, filtered to the managed registry keys.

    Platform-written keys (protocol bookkeeping, auto-enabled features,
    internal counters) never reach the domain, so they can neither trip
    validation nor churn plans. This is backend normalization, owned at the
    adapter boundary like identifier lowercasing and type parsing.

    The ``properties`` field is accepted in both physical shapes it arrives
    in: a native mapping (Spark rows, arrow-native connector results) or a
    JSON-encoded object string (databricks-sql-connector without
    arrow-native complex types). A NULL/empty field means no properties; a
    *missing* field raises, because DESCRIBE DETAIL always carries
    ``properties`` for a Delta table and its absence is a read gone wrong,
    not an empty map.
    """
    properties = _properties_from_detail_row(row)
    managed = {name: value for name, value in properties.items() if name in DELTA_PROPERTY_REGISTRY}
    return MappingProxyType(managed)


def _properties_from_detail_row(row: Any) -> dict[str, str]:
    """Return the raw ``properties`` map from a DESCRIBE DETAIL row."""
    raw = row.properties
    if not raw:
        return {}
    if isinstance(raw, str):
        return json.loads(raw)
    return dict(raw)


def clustering_columns_from_detail_row(row: Any) -> tuple[str, ...]:
    """
    Clustering key column names from a DESCRIBE DETAIL row (empty when unclustered).

    ``clusteringColumns`` is read via ``asDict().get`` so its absence (older
    Delta) yields no clustering rather than breaking the read. The value
    arrives as a native array (Spark rows, arrow-native connector results)
    or a JSON-encoded array string (databricks-sql-connector without
    arrow-native complex types); both are accepted. Names are casefolded to
    match the domain's lowercase columns.
    """
    raw = row.asDict().get("clusteringColumns")
    if not raw:
        return ()
    names = json.loads(raw) if isinstance(raw, str) else raw
    return tuple(name.casefold() for name in names)
