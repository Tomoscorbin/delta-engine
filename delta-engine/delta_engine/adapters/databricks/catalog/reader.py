"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
import pyspark.sql.utils as sku

from delta_engine.adapters.databricks.policy import DEFAULT_PROPERTY_POLICY, PropertyPolicy
from delta_engine.adapters.databricks.sql.preview import error_preview
from delta_engine.adapters.databricks.sql.read import query_describe_detail, query_table_existence
from delta_engine.adapters.databricks.sql.types import domain_type_from_spark
from delta_engine.application.results import ReadFailure, ReadResult
from delta_engine.domain.model import Column, ObservedTable, QualifiedName

_SPARK_EXCEPTION = (
    *tuple(
        exc
        for name in (
            "AnalysisException",
            "ParseException",
            "NoSuchTableException",
            "NoSuchNamespaceException",
            "AuthorizationException",
            "SparkException",
        )
        if (exc := getattr(sku, name, None)) is not None
    ),
    Py4JJavaError,
)


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


@runtime_checkable
class SparkColumn(Protocol):
    """Shape of Spark catalog columns we care about (duck-typed)."""

    name: str
    dataType: Any  # noqa: N815
    nullable: bool | None
    description: str | None
    isPartition: bool | None  # noqa: N815


def _to_domain_column(column: SparkColumn, type_mapper=domain_type_from_spark) -> Column:
    """Convert a spark Column object into a domain `Column`."""
    domain_data_type = type_mapper(column.dataType)
    is_nullable = bool(getattr(column, "nullable", True))
    comment = column.description if column.description else ""

    return Column(
        name=column.name,
        data_type=domain_data_type,
        is_nullable=is_nullable,
        comment=comment,
    )


class DatabricksReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(
        self,
        spark: SparkSession,
        property_policy: PropertyPolicy = DEFAULT_PROPERTY_POLICY,
    ) -> None:
        """Initialize the reader with a `SparkSession`."""
        self.spark = spark
        self._property_policy = property_policy

    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        """
        Fetch observed table schema or absence for a qualified name.

        Returns a successful `ReadResult` with the current columns,
        properties, and table comment; an absent result when the table
        doesn't exist; or a failed result if catalog access raised an
        exception.
        """
        if not self._table_exists(qualified_name):
            return ReadResult.create_absent()

        try:
            columns = self._fetch_columns(str(qualified_name))
            properties = self._fetch_properties(qualified_name)
            table_comment = self._fetch_table_comment(str(qualified_name))
            partition_columns = self._fetch_partition_columns(str(qualified_name))
        except _SPARK_EXCEPTION as exc:
            failure = ReadFailure(type(exc).__name__, error_preview(exc))
            return ReadResult.create_failed(failure)

        observed = ObservedTable(
            qualified_name=qualified_name,
            columns=columns,
            comment=table_comment,
            properties=properties,
            partitioned_by=partition_columns,
        )
        return ReadResult.create_present(observed)

    def _table_exists(self, qualified_name: QualifiedName) -> bool:
        """Return `True` if the table exists, else `False`."""
        query = query_table_existence(qualified_name)
        return bool(self.spark.sql(query).head(1))

    def _fetch_columns(self, fully_qualified_name: str) -> tuple[Column, ...]:
        """List column definitions for the given table."""
        catalog_columns = self.spark.catalog.listColumns(fully_qualified_name)
        return tuple(_to_domain_column(column) for column in catalog_columns)

    def _fetch_partition_columns(self, fully_qualified_name: str) -> tuple[str, ...]:
        """Return a tuple of partition columns."""
        catalog_columns = self.spark.catalog.listColumns(fully_qualified_name)
        return tuple(c.name for c in catalog_columns if bool(getattr(c, "isPartition", False)))

    def _fetch_properties(self, qualified_name: QualifiedName) -> MappingProxyType[str, str]:
        """Return table properties as a read-only mapping."""
        query = query_describe_detail(qualified_name)
        df = self.spark.sql(query)
        row = df.first()
        if not row:
            return MappingProxyType({})
        return self._property_policy.enforce(row["properties"])

    def _fetch_table_comment(self, fully_qualified_name: str) -> str:
        """Return the table comment (empty string when not set)."""
        return self.spark.catalog.getTable(fully_qualified_name).description or ""
