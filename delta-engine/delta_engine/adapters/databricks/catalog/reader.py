"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from pyspark.sql import SparkSession
import pyspark.sql.Column

from delta_engine.adapters.databricks.preview import error_preview
from delta_engine.adapters.databricks.sql.catalog_read import query_table_properties
from delta_engine.adapters.databricks.sql.types import domain_type_from_spark
from delta_engine.application.results import ReadFailure, ReadResult
from delta_engine.domain.model import Column, ObservedTable, QualifiedName


class DatabricksReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the reader with a `SparkSession`."""
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        """
        Fetch observed table schema or absence for a qualified name.

        Returns a successful `ReadResult` with the current columns, an absent
        result when the table doesn't exist, or a failed result if catalog
        access raised an exception.
        """
        fully_qualified_name = str(qualified_name)
        if not self.spark.catalog.tableExists(fully_qualified_name):  # TODO: verify
            return ReadResult.create_absent()

        try:
            columns = self._fetch_columns(fully_qualified_name)
            properties = self._fetch_properties(qualified_name)
        except Exception as exc:
            failure = ReadFailure(type(exc).__name__, error_preview(exc))
            return ReadResult.create_failed(failure)

        observed = ObservedTable(
            qualified_name,
            columns,
            properties,
        )
        return ReadResult.create_present(observed)

    def _fetch_columns(self, fully_qualified_name: str) -> tuple[Column, ...]:
        """List column definitions for the given table."""
        catalog_columns = self.spark.catalog.listColumns(fully_qualified_name)
        return tuple(self._to_domain_column(column) for column in catalog_columns)

    def _fetch_properties(self, qualified_name: QualifiedName) -> dict[str, str]:
        """
        Return table properties as a dict[str, str].

        Uses `SHOW TBLPROPERTIES` which yields rows: key: string, value: string|NULL.
        """
        query = query_table_properties(qualified_name)
        df = self.spark.sql(query)
        props: dict[str, str] = {}
        for row in df.collect():
            key = row["key"]
            val = row["value"]
            props[key] = "" if val is None else str(val)
        return props

    def _to_domain_column(self, spark_column: pyspark.sql.Column) -> Column:
        """Convert a pyspark.sql.Column object into a domain `Column`."""
        spark_data_type = getattr(spark_column, "dataType", None)
        domain_data_type = domain_type_from_spark(spark_data_type)
        is_nullable = bool(getattr(spark_column, "nullable", True))

        return Column(
            name=spark_column.name,
            data_type=domain_data_type,
            is_nullable=is_nullable,
        )
