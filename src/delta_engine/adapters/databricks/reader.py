"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from itertools import groupby
import logging
from types import MappingProxyType

from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import Row, SparkSession
from pyspark.sql.catalog import Column as SparkColumn
from pyspark.sql.types import DataType as SparkType

from delta_engine.adapters.databricks.errors import summarize_exception
from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_detail_query,
    domain_type_from_spark,
    foreign_keys_query,
    information_schema_probe_query,
    primary_key_query,
    table_tags_query,
)
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import (
    CatalogState,
    ReadFailed,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.domain.model import (
    Column as DomainColumn,
    ForeignKeyConstraint,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _ColumnMapping:
    column: DomainColumn
    is_partition: bool


def _to_column_mapping(
    spark_column: SparkColumn, qualified_name: QualifiedName
) -> _ColumnMapping | None:
    """
    Convert a Spark catalog column into a domain ``Column`` and its partition flag.

    Returns ``None`` for columns whose Spark type has no domain mapping yet,
    logging a warning so operators can track gaps as new Spark types are released.

    The column name is lowercased here: the domain model requires lowercase
    identifiers, and case-preserving catalogs (e.g. Hive Metastore) can return
    mixed-case names. Normalising at the adapter boundary keeps that impedance
    mismatch out of the domain. The partition name in ``_ColumnMapping`` is
    therefore derived from the already-normalised domain column name, not from
    the raw Spark object.

    Unity Catalog reports a column's type as a DDL string (e.g. ``"array<int>"``);
    parsing that into a ``SparkType`` is this adapter's job, so the domain-type
    mapper receives a parsed instance.
    """
    domain_data_type = domain_type_from_spark(SparkType.fromDDL(spark_column.dataType))
    if domain_data_type is None:
        logger.warning(
            "Skipping column %r in %s: unrecognised Spark type %r"
            " — the column is invisible to this sync; if a declaration includes"
            " it, the planned ADD COLUMN will fail at execution because the"
            " column already exists",
            spark_column.name,
            qualified_name,
            spark_column.dataType,
        )
        return None

    nullable = bool(getattr(spark_column, "nullable", True))
    comment = spark_column.description if spark_column.description else ""

    return _ColumnMapping(
        column=DomainColumn(
            name=spark_column.name.casefold(),
            data_type=domain_data_type,
            nullable=nullable,
            comment=comment,
        ),
        is_partition=bool(getattr(spark_column, "isPartition", False)),
    )


def _primary_key_from_rows(rows: Sequence[Row]) -> PrimaryKeyConstraint | None:
    """
    Build the primary key from its information_schema rows, or ``None``.

    Constraint and column names are normalised to lowercase at the adapter
    boundary.
    """
    columns = tuple(row["column_name"].casefold() for row in rows)
    if not columns:
        return None
    constraint_name = rows[0]["constraint_name"].casefold()
    return PrimaryKeyConstraint(columns=columns, constraint_name=constraint_name)


def _foreign_keys_from_rows(rows: Iterable[Row]) -> tuple[ForeignKeyConstraint, ...]:
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


def _foreign_key_from_rows(constraint_name: str, rows: list[Row]) -> ForeignKeyConstraint:
    """
    Build one foreign key constraint from its key_column_usage rows.

    ``rows`` are all rows for a single constraint, ordered by
    ordinal_position, so local and referenced columns stay positionally
    aligned. The referenced table is identical on every row, so it is read
    from the first. Column and table names are lowercased at the adapter
    boundary, consistent with the rest of this reader. Constraint names are
    read from the catalog so observed-only constraints can be dropped by
    their real names.
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


def _table_tags_from_rows(rows: Iterable[Row]) -> MappingProxyType[str, str]:
    """Map table-tag rows to a read-only mapping; tag case is preserved verbatim."""
    return MappingProxyType({row.tag_name: row.tag_value for row in rows})


def _column_tags_from_rows(
    rows: Iterable[Row],
) -> MappingProxyType[str, MappingProxyType[str, str]]:
    """
    Map column-tag rows to ``{column_name: {tag: value}}``.

    Column names are casefolded to match the domain's lowercase columns; tag
    keys and values are case-sensitive and returned verbatim — never casefolded.
    """
    grouped: dict[str, dict[str, str]] = {}
    for row in rows:
        grouped.setdefault(row.column_name.casefold(), {})[row.tag_name] = row.tag_value
    return MappingProxyType({column: MappingProxyType(tags) for column, tags in grouped.items()})


def _managed_properties_from_row(row: Row) -> MappingProxyType[str, str]:
    """
    Filter a DESCRIBE DETAIL row's properties to the managed registry keys.

    Platform-written keys (protocol bookkeeping, auto-enabled features,
    internal counters) never reach the domain, so they can neither trip
    validation nor churn plans. This is backend normalization, owned here
    like identifier lowercasing and type parsing.
    """
    observed_properties = {
        name: value
        for name, value in dict(row["properties"]).items()
        if name in DELTA_PROPERTY_REGISTRY
    }
    return MappingProxyType(observed_properties)


class DatabricksReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the reader with a `SparkSession`."""
        self.spark = spark
        self._information_schema_availability: dict[str, bool] = {}

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """
        Fetch the current state of a table: present, absent, or unreadable.

        Returns ``TablePresent`` carrying the current observed table snapshot;
        ``TableAbsent`` when the table doesn't exist; or
        ``ReadFailed`` if catalog access raised an exception.

        Every failure mode is contained: anything that goes wrong reading this
        table -- a failing existence probe, an unsupported column type, a Spark
        error mid-read -- becomes a ``ReadFailed`` for this table rather than an
        exception that aborts the whole sync. The ``CatalogStateReader`` contract
        promises a ``CatalogState``, so the boundary must be total.
        """
        try:
            return self._read(qualified_name)
        except Exception as exception:
            summary = summarize_exception(exception)
            return ReadFailed(failure=ReadFailure(summary.type_name, summary.message))

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        """Read current state, letting any failure propagate to ``fetch_state``."""
        if not self._table_exists(qualified_name):
            return TableAbsent()
        catalog = qualified_name.catalog

        all_mappings = (
            _to_column_mapping(c, qualified_name)
            for c in self.spark.catalog.listColumns(str(qualified_name))
        )
        mappings = tuple(m for m in all_mappings if m is not None)
        column_tags = _column_tags_from_rows(
            self._information_schema_rows(catalog, column_tags_query(qualified_name))
        )
        columns = tuple(
            replace(m.column, tags=column_tags.get(m.column.name, MappingProxyType({})))
            for m in mappings
        )
        observed = ObservedTable(
            qualified_name=qualified_name,
            columns=columns,
            comment=self._fetch_table_comment(qualified_name),
            properties=_managed_properties_from_row(self._describe_detail_row(qualified_name)),
            tags=_table_tags_from_rows(
                self._information_schema_rows(catalog, table_tags_query(qualified_name))
            ),
            partitioned_by=tuple(m.column.name for m in mappings if m.is_partition),
            primary_key=_primary_key_from_rows(
                self._information_schema_rows(catalog, primary_key_query(qualified_name))
            ),
            foreign_keys=_foreign_keys_from_rows(
                self._information_schema_rows(catalog, foreign_keys_query(qualified_name))
            ),
        )
        return TablePresent(table=observed)

    def _table_exists(self, qualified_name: QualifiedName) -> bool:
        """
        Return `True` if the table exists, else `False`.

        Uses the catalog's own existence check rather than a hand-rolled
        information_schema query. Note: ``tableExists`` also reports ``True`` for
        a session temporary view registered under this name; that is an unusual
        collision for a three-part ``catalog.schema.table`` and, if it happened,
        the subsequent read would surface as a ``ReadFailed`` rather than corrupt
        state.
        """
        return self.spark.catalog.tableExists(str(qualified_name))

    def _describe_detail_row(self, qualified_name: QualifiedName) -> Row:
        """
        Return the table's DESCRIBE DETAIL row.

        Raises when the query yields no row for a table the existence probe
        just reported present: an empty result there is not "a table with no
        properties" (that is a present row with an empty map) but a race or a
        catalog inconsistency. Failing loud lets ``fetch_state``'s error
        boundary return ``ReadFailed`` — the honest outcome for "could not
        determine state" — rather than a ``TablePresent`` with no properties,
        which would make the differ re-apply every managed property on every
        sync.
        """
        row = self.spark.sql(describe_detail_query(qualified_name)).first()
        if row is None:
            raise RuntimeError(
                f"DESCRIBE DETAIL returned no rows for {qualified_name}, which the"
                " existence probe just reported as present — the table was dropped"
                " mid-read or the catalog is inconsistent."
            )
        return row

    def _information_schema_rows(self, catalog: str, query: str) -> list[Row]:
        """
        Run a Unity Catalog information_schema query and return its rows.

        Where ``catalog`` has no information_schema (plain Spark, e.g. local
        tests), returns no rows without querying: the views' absence is probed
        once per catalog and cached, so there is no per-query exception
        heuristic. Where information_schema exists, failures propagate to
        ``fetch_state``'s boundary and become ``ReadFailed`` — a permission
        error or query regression must not masquerade as "no constraints, no
        tags", which would churn tags on every sync and plan constraint DDL
        that fails at execution.
        """
        if not self._information_schema_available(catalog):
            return []
        return self.spark.sql(query).collect()

    def _information_schema_available(self, catalog: str) -> bool:
        """Probe (once per catalog) whether information_schema is queryable."""
        if catalog not in self._information_schema_availability:
            try:
                self.spark.sql(information_schema_probe_query(catalog)).collect()
            except AnalysisException:
                self._information_schema_availability[catalog] = False
            else:
                self._information_schema_availability[catalog] = True
        return self._information_schema_availability[catalog]

    def _fetch_table_comment(self, qualified_name: QualifiedName) -> str:
        """Return the table comment (empty string when not set)."""
        return self.spark.catalog.getTable(str(qualified_name)).description or ""
