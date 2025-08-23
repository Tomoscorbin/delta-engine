"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from tabula.adapters.databricks.sql.types import domain_type_from_spark
from tabula.domain.model import Column, ObservedTable, QualifiedName


@dataclass(frozen=True, slots=True)
class UCReader:
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

    def _table_exists(self, qualified_name: QualifiedName) -> bool:
        return self.spark.catalog.tableExists(qualified_name.dotted)

    def _list_columns(self, qualified_name: QualifiedName) -> tuple[Column, ...]:
        cols = self.spark.catalog.listColumns(qualified_name.dotted)
        out: list[Column] = []
        for c in cols:
            out.append(
                Column(
                    name=c.name,
                    data_type=domain_type_from_spark(c.dataType),
                    is_nullable=c.nullable,
                )
            )
        return tuple(out)

    def _is_table_empty(self, qualified_name: QualifiedName) -> bool:
        return self.spark.table(qualified_name.dotted).isEmpty()
