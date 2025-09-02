"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from types import MappingProxyType

from pyspark.sql import SparkSession
from pyspark.sql.catalog import Column as SparkColumn

from delta_engine.adapters.databricks.policy import enforce_property_policy
from delta_engine.adapters.databricks.preview import error_preview
from delta_engine.adapters.databricks.sql.read import query_describe_detail, query_table_existence
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
        if not self._table_exists(qualified_name):
            return ReadResult.create_absent()

        try:
            columns = self._fetch_columns(str(qualified_name))
            properties = self._fetch_properties(qualified_name)
        except Exception as exc:  # TODO: need more accurate exception catching
            failure = ReadFailure(type(exc).__name__, error_preview(exc))
            return ReadResult.create_failed(failure)

        observed = ObservedTable(
            qualified_name,
            columns,
            properties,
        )
        return ReadResult.create_present(observed)

    def _table_exists(self, qualified_name: QualifiedName) -> bool:
        query = query_table_existence(qualified_name)
        return bool(self.spark.sql(query).head(1))

    def _fetch_columns(self, fully_qualified_name: str) -> tuple[Column, ...]:
        """List column definitions for the given table."""
        catalog_columns = self.spark.catalog.listColumns(fully_qualified_name)
        return tuple(self._to_domain_column(column) for column in catalog_columns)

    def _fetch_properties(self, qualified_name: QualifiedName) -> MappingProxyType[str, str]:
        """Return table properties as a dict[str, str]."""
        query = query_describe_detail(qualified_name)
        df = self.spark.sql(query)
        row = df.first()
        if not row:
            return MappingProxyType({})
        return enforce_property_policy(
            row["properties"]
        )  # Should this be here or in a dedicated enforcement step/method?

    def _to_domain_column(self, spark_column: SparkColumn) -> Column:
        """Convert a pyspark.sql.Column object into a domain `Column`."""
        spark_data_type = spark_column.dataType
        domain_data_type = domain_type_from_spark(spark_data_type)
        is_nullable = bool(getattr(spark_column, "nullable", True))
        comment = spark_column.description if spark_column.description else ""

        return Column(
            name=spark_column.name,
            data_type=domain_data_type,
            is_nullable=is_nullable,
            comment=comment,
        )
