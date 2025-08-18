from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import SparkSession

from tabula.adapters.databricks.sql.dialects import (
    SPARK_SQL,
    SqlDialect,
)
from tabula.adapters.databricks.sql.types import domain_type_from_spark
from tabula.domain.model.column import Column
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.table import ObservedTable


@dataclass(frozen=True, slots=True)
class SparkCatalogReader:
    """
    Unity Catalog reader backed by Spark catalog and tiny SQL probes.

    Public API:
      - fetch_state(...)
    """
    spark: SparkSession
    dialect: SqlDialect = SPARK_SQL

    # ---- public API ---------------------------------------------------------

    def fetch_state(self, qualified_name: QualifiedName) -> Optional[ObservedTable]:
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
        # Spark API is fine for existence checks.
        return self.spark.catalog.tableExists(qualified_name.dotted)

    def _list_columns(self, qualified_name: QualifiedName) -> tuple[Column, ...]:
        # Spark API is reliable and faster than DESCRIBE.
        cols = self.spark.catalog.listColumns(qualified_name.dotted)
        out: list[Column] = []
        for c in cols:
            out.append(
                Column(
                    name=c.name,
                    data_type=domain_type_from_spark(c.dataType),
                    is_nullable=bool(c.nullable),
                )
            )
        return tuple(out)

    def _is_table_empty(self, qualified_name: QualifiedName) -> bool:
        return self.spark.table(qualified_name.dotted).isEmpty()
