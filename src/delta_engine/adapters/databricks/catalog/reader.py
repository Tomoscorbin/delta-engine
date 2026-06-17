"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from types import MappingProxyType

from py4j.protocol import Py4JJavaError  # type: ignore[import]
from pyspark.sql import SparkSession
from pyspark.sql.catalog import Column as SparkColumn

from delta_engine.adapters.databricks.sql.preview import error_preview
from delta_engine.adapters.databricks.sql.read import query_describe_detail, query_table_existence
from delta_engine.adapters.databricks.sql.types import domain_type_from_spark
from delta_engine.application.results import ReadFailed, ReadFailure, ReadResult, ReadSucceeded
from delta_engine.domain.model import Column as DomainColumn, ObservedTable, QualifiedName


def _exc_type_name(exc: Exception) -> str:
    """Prefer the underlying Java class for Py4J errors; else the Python class."""
    if isinstance(exc, Py4JJavaError):
        try:
            return (
                exc.java_exception.getClass().getName()
            )  # e.g. 'org.apache.spark.sql.AnalysisException'
        except Exception:
            return "Py4JJavaError"
    return type(exc).__name__


def _to_domain_column(column: SparkColumn, type_mapper=domain_type_from_spark) -> DomainColumn:
    """
    Convert a spark Column object into a domain `Column`.

    The column name is lowercased here: the domain model requires lowercase
    identifiers, and case-preserving catalogs (e.g. Hive Metastore) can return
    mixed-case names. Normalising at the adapter boundary keeps that impedance
    mismatch out of the domain.
    """
    domain_data_type = type_mapper(column.dataType)
    nullable = bool(getattr(column, "nullable", True))
    comment = column.description if column.description else ""

    return DomainColumn(
        name=column.name.casefold(),
        data_type=domain_data_type,
        nullable=nullable,
        comment=comment,
    )


class DatabricksReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the reader with a `SparkSession`."""
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        """
        Fetch observed table schema or absence for a qualified name.

        Returns ``ReadSucceeded`` carrying the current columns, properties, and
        table comment; ``ReadSucceeded`` with ``observed=None`` when the table
        doesn't exist; or ``ReadFailed`` if catalog access raised an exception.

        Every failure mode is contained: anything that goes wrong reading this
        table -- a failing existence probe, an unsupported column type, a Spark
        error mid-read -- becomes a ``ReadFailed`` for this table rather than an
        exception that aborts the whole sync. The ``CatalogStateReader`` contract
        promises a ``ReadResult``, so the boundary must be total.
        """
        try:
            return self._read(qualified_name)
        except Exception as exc:
            failure = ReadFailure(_exc_type_name(exc), error_preview(exc))
            return ReadFailed(failure=failure)

    def _read(self, qualified_name: QualifiedName) -> ReadResult:
        """Read current state, letting any failure propagate to ``fetch_state``."""
        if not self._table_exists(qualified_name):
            return ReadSucceeded(observed=None)

        catalog_columns = self.spark.catalog.listColumns(str(qualified_name))
        columns = tuple(_to_domain_column(c) for c in catalog_columns)
        partition_columns = tuple(
            c.name.casefold() for c in catalog_columns if bool(getattr(c, "isPartition", False))
        )
        observed = ObservedTable(
            qualified_name=qualified_name,
            columns=columns,
            comment=self._fetch_table_comment(qualified_name),
            properties=self._fetch_properties(qualified_name),
            partitioned_by=partition_columns,
        )
        return ReadSucceeded(observed=observed)

    def _table_exists(self, qualified_name: QualifiedName) -> bool:
        """Return `True` if the table exists, else `False`."""
        query = query_table_existence(qualified_name)
        return bool(self.spark.sql(query).head(1))

    def _fetch_properties(self, qualified_name: QualifiedName) -> MappingProxyType[str, str]:
        """Return all catalog table properties as a read-only mapping."""
        query = query_describe_detail(qualified_name)
        df = self.spark.sql(query)
        row = df.first()
        if not row:
            return MappingProxyType({})
        return MappingProxyType(dict(row["properties"]))

    def _fetch_table_comment(self, qualified_name: QualifiedName) -> str:
        """Return the table comment (empty string when not set)."""
        return self.spark.catalog.getTable(str(qualified_name)).description or ""
