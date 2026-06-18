"""Factory for constructing an `Engine` wired to Databricks/Spark adapters."""

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.executor import DatabricksExecutor
from delta_engine.adapters.databricks.reader import DatabricksReader
from delta_engine.application.engine import Engine


def build_databricks_engine(spark: SparkSession) -> Engine:
    """
    Create an engine configured for Databricks.

    This is the public entry point for end users. It has no logging side effect:
    for the common script/notebook case, call :func:`configure_logging` once at
    startup to install the package's coloured handler; embedders who own their
    own logging simply leave it alone.

    Args:
        spark: The Spark session the reader and executor run against.

    """
    reader = DatabricksReader(spark)
    executor = DatabricksExecutor(spark)
    return Engine(reader=reader, executor=executor)
