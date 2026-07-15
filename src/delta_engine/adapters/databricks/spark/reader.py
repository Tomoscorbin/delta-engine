"""
Reader adapter for Databricks Unity Catalog over a SparkSession.

Unity Catalog only. One ``DESCRIBE TABLE EXTENDED … AS JSON`` yields the
table-local state; three information_schema queries add tags and inbound
foreign keys. Shares all parsing and assembly with the warehouse backend;
only statement execution and error classification are backend-specific.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.errors import (
    exception_message,
    exception_type_name,
    is_missing_relation,
)
from delta_engine.adapters.databricks.read import observed_table_from_snapshot
from delta_engine.adapters.databricks.sql import describe_json_query
from delta_engine.adapters.databricks.sql.describe_json import parse_table_snapshot
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName


class SparkReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """Return ``TablePresent``, ``TableAbsent``, or ``ReadFailed`` — the boundary is total."""
        try:
            return self._read(qualified_name)
        except Exception as exception:
            return ReadFailed(
                failure=ReadFailure(exception_type_name(exception), exception_message(exception))
            )

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        try:
            row = self.spark.sql(describe_json_query(qualified_name)).first()
        except Exception as exception:
            if is_missing_relation(exception):
                return TableAbsent()
            raise
        if row is None:
            raise RuntimeError(f"DESCRIBE AS JSON returned no row for {qualified_name}")
        snapshot = parse_table_snapshot(row[0], qualified_name)
        observed = observed_table_from_snapshot(
            snapshot, run_info_schema_query=lambda query: self.spark.sql(query).collect()
        )
        return TablePresent(table=observed)
