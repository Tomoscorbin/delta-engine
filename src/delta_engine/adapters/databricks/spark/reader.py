"""
Reader adapter for Databricks Unity Catalog over a SparkSession.

Unity Catalog only. Reads one table's state through the shared
``read_catalog_state`` (one ``DESCRIBE TABLE EXTENDED … AS JSON`` plus three
information_schema queries). This backend supplies only how a query runs —
``spark.sql(...).collect()`` — while parsing, assembly, error classification,
and the total boundary are shared with the warehouse backend.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.read import read_catalog_state
from delta_engine.application.ports import CatalogState
from delta_engine.domain.model import QualifiedName


class SparkReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """Return ``TablePresent``, ``TableAbsent``, or ``ReadFailed`` — the boundary is total."""
        return read_catalog_state(lambda sql: self.spark.sql(sql).collect(), qualified_name)
