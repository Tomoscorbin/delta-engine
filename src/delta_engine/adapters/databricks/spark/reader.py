"""Reader adapter for Databricks Unity Catalog."""

from dataclasses import dataclass
from typing import Final

from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import Row, SparkSession
from pyspark.sql.catalog import Column as SparkColumn

from delta_engine.adapters.databricks.errors import exception_message, exception_type_name
from delta_engine.adapters.databricks.read import observed_table_from_reads
from delta_engine.adapters.databricks.sql import (
    column_from_catalog,
    describe_detail_query,
    information_schema_probe_query,
    parse_data_type,
)
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import (
    CatalogState,
    ReadFailed,
    TableAbsent,
    TablePresent,
)
from delta_engine.domain.model import (
    ObservedColumn,
    QualifiedName,
)

# AnalysisException conditions that mean a catalog has no information_schema
# (plain Spark, Hive metastore). TABLE_OR_VIEW_NOT_FOUND is what Spark raises
# for the probe; SCHEMA_NOT_FOUND covers resolvers that report the missing
# schema instead. Anything else — a permission error, a transient failure — is
# not evidence of absence and must propagate.
_INFORMATION_SCHEMA_MISSING_CONDITIONS: Final[frozenset[str]] = frozenset(
    {"TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND"}
)


@dataclass(frozen=True, slots=True)
class _ColumnMapping:
    column: ObservedColumn
    is_partition: bool


def _to_column_mapping(
    spark_column: SparkColumn, qualified_name: QualifiedName
) -> _ColumnMapping | None:
    """
    Convert a Spark catalog column into an ``ObservedColumn`` and its partition flag.

    Spark reports a column's type as a DDL string (e.g. ``"array<int>"``).
    This function parses that source-specific representation, then both readers
    share normalization and the unmappable-column policy through
    ``column_from_catalog`` (skip and warn; raise for partition columns).

    The partition name in ``_ColumnMapping`` is derived from the
    already-normalised domain column name, not from the raw Spark object,
    because case-preserving catalogs (e.g. Hive Metastore) can return
    mixed-case names.
    """
    # Catalog column objects are duck-typed: some catalog implementations omit
    # the nullable/isPartition flags, so a missing flag means "nullable" /
    # "not a partition" rather than an error.
    is_partition = bool(getattr(spark_column, "isPartition", False))
    reported_type = spark_column.dataType
    column = column_from_catalog(
        name=spark_column.name,
        data_type=parse_data_type(reported_type),
        reported_type=reported_type,
        nullable=bool(getattr(spark_column, "nullable", True)),
        comment=spark_column.description or "",
        is_partition=is_partition,
        qualified_name=qualified_name,
    )
    if column is None:
        return None
    return _ColumnMapping(column=column, is_partition=is_partition)


class SparkReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._information_schema_availability: dict[str, bool] = {}

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """
        Fetch the current state of a table: present, absent, or unreadable.

        Returns ``TablePresent`` carrying the current observed table snapshot;
        ``TableAbsent`` when the table doesn't exist; or
        ``ReadFailed`` if any part of the read raised an exception.

        Every exception raised while reading -- such as a failing existence
        probe, an unsupported partition-column type, or a Spark error mid-read
        -- becomes a ``ReadFailed`` for this table rather than aborting the
        whole sync. Unmappable non-partition columns are skipped by the mapper
        and do not raise. The ``CatalogStateReader`` contract promises a
        ``CatalogState``, so the boundary must be total.
        """
        try:
            return self._read(qualified_name)
        except Exception as exception:
            return ReadFailed(
                failure=ReadFailure(exception_type_name(exception), exception_message(exception))
            )

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        """Read current state, letting any failure propagate to ``fetch_state``."""
        if not self._table_exists(qualified_name):
            return TableAbsent()
        catalog = qualified_name.catalog

        candidate_mappings = (
            _to_column_mapping(spark_column, qualified_name)
            for spark_column in self.spark.catalog.listColumns(str(qualified_name))
        )
        mappings = tuple(mapping for mapping in candidate_mappings if mapping is not None)
        observed = observed_table_from_reads(
            qualified_name,
            columns=tuple(mapping.column for mapping in mappings),
            comment=self._fetch_table_comment(qualified_name),
            detail_row=self._describe_detail_row(qualified_name),
            partitioned_by=tuple(
                mapping.column.name for mapping in mappings if mapping.is_partition
            ),
            run_info_schema_query=lambda query: self._information_schema_rows(catalog, query),
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
        """
        Probe (once per catalog) whether information_schema is queryable.

        Only the missing-view conditions may conclude "unavailable" — a
        permission error or transient failure on the probe propagates to
        ``fetch_state``'s boundary as ``ReadFailed`` rather than reading the
        whole catalog as constraint- and tag-free (the same policy
        ``_information_schema_rows`` applies to the metadata queries). An
        unexpected failure is not cached, so the next read probes again.
        """
        if catalog not in self._information_schema_availability:
            try:
                self.spark.sql(information_schema_probe_query(catalog)).collect()
            except AnalysisException as exception:
                if exception.getCondition() not in _INFORMATION_SCHEMA_MISSING_CONDITIONS:
                    raise
                self._information_schema_availability[catalog] = False
            else:
                self._information_schema_availability[catalog] = True
        return self._information_schema_availability[catalog]

    def _fetch_table_comment(self, qualified_name: QualifiedName) -> str:
        """Return the table comment (empty string when not set)."""
        return self.spark.catalog.getTable(str(qualified_name)).description or ""
