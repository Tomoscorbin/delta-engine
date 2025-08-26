from pyspark.sql import SparkSession

from tabula.adapters.databricks.catalog.executor import DatabricksExecutor
from tabula.adapters.databricks.catalog.reader import DatabricksReader
from tabula.application.engine import Engine


def build_databricks_engine(spark: SparkSession) -> Engine:
    """Public factory for end users to create an engine configured for Databricks."""
    reader = DatabricksReader(spark)
    executor = DatabricksExecutor(spark)
    return Engine(reader=reader, executor=executor)
