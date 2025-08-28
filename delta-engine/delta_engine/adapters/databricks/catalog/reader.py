"""Reader adapter for Databricks Unity Catalog."""

from __future__ import annotations

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.sql.dialect import quote_literal
from delta_engine.adapters.databricks.sql.types import domain_type_from_spark
from delta_engine.domain.model import Column, ObservedTable, QualifiedName


class DatabricksReader:
    spark: SparkSession

    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None:
        if not self._table_exists(qualified_name):
            return None

        columns = self._list_columns(qualified_name)

        return ObservedTable(
            qualified_name=qualified_name,
            columns=columns,
        )

    # ---- private helpers ----------------------------------------------------

    def _table_exists(self, qualified_name: QualifiedName) -> bool: #TODO: put sql in compiler
        sql = f"""
        SELECT 1
        FROM {quote_literal(qualified_name.catalog)}.information_schema.tables
        WHERE table_schema = '{quote_literal(qualified_name.schema)}'
            AND table_name   = '{quote_literal(qualified_name.name)}'
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
