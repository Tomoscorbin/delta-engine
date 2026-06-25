"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType

from pyspark.sql import SparkSession
from pyspark.sql.catalog import Column as SparkColumn

from delta_engine.adapters.databricks.sql import (
    backtick_qualified_name,
    domain_type_from_spark,
    error_preview,
    exception_type_name,
)
from delta_engine.application.results import (
    CatalogState,
    ReadFailed,
    ReadFailure,
    TableAbsent,
    TablePresent,
)
from delta_engine.domain.model import Column as DomainColumn, ObservedTable, QualifiedName


@dataclass(frozen=True, slots=True)
class _ColumnMapping:
    column: DomainColumn
    is_partition: bool


def _to_column_mapping(spark_column: SparkColumn) -> _ColumnMapping:
    """
    Convert a Spark catalog column into a domain ``Column`` and its partition flag.

    The column name is lowercased here: the domain model requires lowercase
    identifiers, and case-preserving catalogs (e.g. Hive Metastore) can return
    mixed-case names. Normalising at the adapter boundary keeps that impedance
    mismatch out of the domain. The partition name in ``_ColumnMapping`` is
    therefore derived from the already-normalised domain column name, not from
    the raw Spark object.
    """
    domain_data_type = domain_type_from_spark(spark_column.dataType)
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


class DatabricksReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the reader with a `SparkSession`."""
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """
        Fetch the current state of a table: present, absent, or unreadable.

        Returns ``TablePresent`` carrying the current columns, properties, and
        table comment; ``TableAbsent`` when the table doesn't exist; or
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
            failure = ReadFailure(exception_type_name(exception), error_preview(exception))
            return ReadFailed(failure=failure)

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        """Read current state, letting any failure propagate to ``fetch_state``."""
        if not self._table_exists(qualified_name):
            return TableAbsent()

        mappings = tuple(
            _to_column_mapping(c) for c in self.spark.catalog.listColumns(str(qualified_name))
        )
        columns = tuple(m.column for m in mappings)
        partition_columns = tuple(m.column.name for m in mappings if m.is_partition)
        observed = ObservedTable(
            qualified_name=qualified_name,
            columns=columns,
            comment=self._fetch_table_comment(qualified_name),
            properties=self._fetch_properties(qualified_name),
            partitioned_by=partition_columns,
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

    def _fetch_properties(self, qualified_name: QualifiedName) -> MappingProxyType[str, str]:
        """Return all catalog table properties as a read-only mapping."""
        # The name is interpolated into SQL text here, so it must be backtick-quoted
        # to stay an identifier (and escape any embedded backtick). This differs
        # deliberately from the catalog.* calls, which take the plain ``str()`` form
        # because they parse the dot-separated parts themselves. Don't unify the two.
        query = f"DESCRIBE DETAIL {backtick_qualified_name(qualified_name)}"
        row = self.spark.sql(query).first()
        if not row:
            return MappingProxyType({})
        return MappingProxyType(dict(row["properties"]))

    def _fetch_table_comment(self, qualified_name: QualifiedName) -> str:
        """Return the table comment (empty string when not set)."""
        return self.spark.catalog.getTable(str(qualified_name)).description or ""
