import os
import sys

from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark() -> SparkSession:  # type: ignore[misc]
    """Minimal, fast SparkSession for tests."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("delta-engine-tests")
        # Kill anything non-essential
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
        .config("spark.sql.streaming.ui.enabled", "false")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.shuffle.service.enabled", "false")
        .config("spark.speculation", "false")
        # Make jobs tiny + consistent
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        # Avoid Hive; use in-memory catalog
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        # Keep networking trivial
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        # Short, sane timeouts
        .config("spark.network.timeout", "60s")
        .config("spark.executor.heartbeatInterval", "30s")
    )

    spark = builder.getOrCreate()

    try:
        yield spark
    finally:
        spark.stop()
