"""Factory for constructing an `Engine` wired to Databricks/Spark adapters."""

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.spark._runner import SparkSqlRunner
from delta_engine.adapters.databricks.spark.executor import SparkExecutor
from delta_engine.adapters.databricks.spark.reader import SparkReader
from delta_engine.adapters.databricks.spark.runtime import (
    require_supported_databricks_runtime,
)
from delta_engine.application.engine import Engine


def build_engine(spark: SparkSession) -> Engine:
    """
    Create an engine configured for Databricks.

    This is the public entry point for end users. It has no logging side effect:
    for the common script/notebook case, call :func:`configure_logging` once at
    startup to install the package's coloured handler; embedders who own their
    own logging simply leave it alone.
    """
    require_supported_databricks_runtime(spark)
    runner = SparkSqlRunner(spark)
    reader = SparkReader(runner)
    executor = SparkExecutor(runner)
    return Engine(reader=reader, executor=executor)
