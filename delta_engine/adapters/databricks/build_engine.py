"""Factory for constructing an `Engine` wired to Databricks/Spark adapters."""

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.catalog.executor import DatabricksExecutor
from delta_engine.adapters.databricks.catalog.reader import DatabricksReader
from delta_engine.application.engine import Engine


def build_databricks_engine(spark: SparkSession) -> Engine:
    """Public factory for end users to create an engine configured for Databricks."""
    reader = DatabricksReader(spark)
    executor = DatabricksExecutor(spark)
    return Engine(reader=reader, executor=executor)
