import os
import shutil
import sys
import tempfile

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import pytest

from tests.config import TEST_CATALOG


@pytest.fixture(scope="session")
def spark() -> SparkSession:  # type: ignore[misc]
    """Minimal, fast SparkSession for tests."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    warehouse_dir = tempfile.mkdtemp(prefix="delta-warehouse-")

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
        # Short timeouts
        .config("spark.network.timeout", "60s")
        .config("spark.executor.heartbeatInterval", "30s")
        # Delta tables
        .config("spark.sql.warehouse.dir", f"file:{warehouse_dir}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            f"spark.sql.catalog.{TEST_CATALOG}", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    # let Delta add the jars + set spark.sql.extensions, etc.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    try:
        yield spark
    finally:
        spark.stop()
        shutil.rmtree(warehouse_dir, ignore_errors=True)
