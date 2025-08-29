"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.errors import DatabricksError
from delta_engine.adapters.databricks.preview import error_preview
from delta_engine.adapters.databricks.sql.types import domain_type_from_spark
from delta_engine.application.results import CatalogReadResult, ReadFailure
from delta_engine.domain.model import Column, ObservedTable, QualifiedName


class DatabricksReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    spark: SparkSession

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogReadResult:
        try:
            columns = self._list_columns(qualified_name)
        except Exception as exc:
            error_class = exc.getCondition()
            failure = ReadFailure(error_class, error_preview)

            if error_class == DatabricksError.TABLE_OR_VIEW_NOT_FOUND:
                return CatalogReadResult.absent()
            return CatalogReadResult.failed(failure)

        observed = ObservedTable(qualified_name, columns)

        return CatalogReadResult.present(observed)

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
