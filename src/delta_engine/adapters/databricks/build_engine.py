"""Factory for constructing an `Engine` wired to Databricks/Spark adapters."""

import logging

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.catalog.executor import DatabricksExecutor
from delta_engine.adapters.databricks.catalog.reader import DatabricksReader
from delta_engine.application.engine import Engine
from delta_engine.log_config import configure_logging


def build_databricks_engine(spark: SparkSession, *, log_level: int = logging.INFO) -> Engine:
    """
    Create an engine configured for Databricks.

    This is the public entry point for end users. It configures package logging
    as a convenience for the common script/notebook use case; embedders who own
    their own logging can call :func:`configure_logging` themselves and wire an
    :class:`Engine` directly.

    Args:
        spark: The Spark session the reader and executor run against.
        log_level: Logging level to install for the run.

    """
    configure_logging(log_level)
    reader = DatabricksReader(spark)
    executor = DatabricksExecutor(spark)
    return Engine(reader=reader, executor=executor)
