import os
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Minimal, fast SparkSession for tests."""
    # Keep Spark fully local and deterministic
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    # Ephemeral warehouse dir so Spark doesn't touch project
    warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
    checkpoint_dir = tempfile.mkdtemp(prefix="spark-checkpoint-")

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
        .config("spark.sql.warehouse.dir", warehouse_dir)
        # Keep networking trivial
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        # Short, sane timeouts
        .config("spark.network.timeout", "60s")
        .config("spark.executor.heartbeatInterval", "30s")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    try:
        yield spark
    finally:
        spark.stop()
        shutil.rmtree(warehouse_dir, ignore_errors=True)
        shutil.rmtree(checkpoint_dir, ignore_errors=True)
