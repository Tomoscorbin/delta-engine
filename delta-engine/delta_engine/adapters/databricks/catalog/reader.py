"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.preview import error_preview
from delta_engine.adapters.databricks.sql.types import domain_type_from_spark
from delta_engine.application.results import ReadFailure, ReadResult
from delta_engine.domain.model import Column, ObservedTable, QualifiedName


class DatabricksReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the reader with a `SparkSession`."""
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        """Fetch observed table schema or absence for a qualified name.

        Returns a successful `ReadResult` with the current columns, an absent
        result when the table doesn't exist, or a failed result if catalog
        access raised an exception.
        """
        fully_qualified_name = str(qualified_name)
        if not self.spark.catalog.tableExists(fully_qualified_name):  # TODO: verify
            return ReadResult.create_absent()

        try:
            columns = self._list_columns(qualified_name)
        except Exception as exc:
            failure = ReadFailure(type(exc).__name__, error_preview(exc))
            return ReadResult.create_failed(failure)

        observed = ObservedTable(qualified_name, columns)
        return ReadResult.create_present(observed)

    def _list_columns(self, qualified_name: QualifiedName) -> tuple[Column, ...]:
        """List column definitions for the given table."""
        catalog_columns = self.spark.catalog.listColumns(str(qualified_name))
        domain_columns = []
        for column in catalog_columns:
            spark_dtype = getattr(column, "dataType", None)
            domain_dtype = domain_type_from_spark(spark_dtype)
            is_nullable = bool(getattr(column, "nullable", True))
            domain_columns.append(
                Column(
                    name=column.name,
                    data_type=domain_dtype,
                    is_nullable=is_nullable,
                )
            )
        return tuple(domain_columns)
