"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.sql.types import domain_type_from_spark
from delta_engine.domain.model import Column, ObservedTable, QualifiedName


@dataclass(frozen=True, slots=True) #TODO: this shouldnt be a dataclass
class DatabricksReader:
    """Unity Catalog reader for Databricks.

    Attributes:
        spark: Active Spark session connected to Databricks.

    """

    spark: SparkSession

    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None:
        """Return catalog information for the given table.

        Args:
            qualified_name: Fully qualified table name.

        Returns:
            Observed table information or ``None`` if the table does not exist.

        """
        if not self._table_exists(qualified_name):
            return None

        columns = self._list_columns(qualified_name)
        is_empty = self._is_table_empty(qualified_name)

        return ObservedTable(
            qualified_name=qualified_name,
            columns=columns,
            is_empty=is_empty,
        )

    # ---- private helpers ----------------------------------------------------

    def _table_exists(self, qualified_name: QualifiedName) -> bool: #TODO: put sql in compiler
        sql = f"""
        SELECT 1
        FROM `{qualified_name.catalog}`.information_schema.tables
        WHERE table_schema = '{qualified_name.schema}'
            AND table_name   = '{qualified_name.name}'
        LIMIT 1
        """
        return bool(self.spark.sql(sql).head(1))

    def _list_columns(self, qualified_name: QualifiedName) -> tuple[Column, ...]:
        cols = self.spark.catalog.listColumns(str(qualified_name))
        out: list[Column] = []
        for c in cols:
            spark_dtype = getattr(c, "dataType", None)
            domain_dtype = domain_type_from_spark(spark_dtype)
            is_nullable = bool(getattr(c, "nullable", True))
            out.append(
                Column(
                    name=c.name,
                    data_type=domain_dtype,
                    is_nullable=is_nullable,
                )
            )
        return tuple(out)

    def _is_table_empty(self, qualified_name: QualifiedName) -> bool:
        return self.spark.table(str(qualified_name)).isEmpty()
