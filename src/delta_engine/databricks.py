"""
Public Databricks entry points.

The implementation lives in ``delta_engine.adapters.databricks``. This module
keeps the user-facing Databricks import path short while preserving lazy
backend imports (PySpark, databricks-sql-connector) for code that only
declares schemas or inspects result types.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TextIO

from delta_engine.application.engine import Engine
from delta_engine.application.ports import CatalogStateReader, DesiredTableSource

if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

__all__ = [
    "build_reader",
    "build_spark_engine",
    "build_sql_engine",
    "configure_logging",
    "to_spark_schema",
]

_SPARK_RUNTIME_HINT = (
    "delta-engine's Spark backend requires the PySpark supplied by a supported Databricks Runtime."
)


def _is_pyspark_import_error(error: ModuleNotFoundError) -> bool:
    """Return whether an import failed while resolving the PySpark namespace."""
    module_name = error.name or ""
    return module_name == "pyspark" or module_name.startswith("pyspark.")


def build_spark_engine(spark: SparkSession) -> Engine:
    """Create an engine that syncs through an active Spark session."""
    try:
        from delta_engine.adapters.databricks.spark.factory import build_engine as _build_engine
    except ModuleNotFoundError as error:
        if _is_pyspark_import_error(error):
            raise RuntimeError(_SPARK_RUNTIME_HINT) from error
        raise

    return _build_engine(spark)


def to_spark_schema(table: DesiredTableSource) -> StructType:
    """
    Convert a table declaration to a native PySpark ``StructType``.

    Preserves column order, authored name spelling, data type, and nullability.
    Comments and tags remain catalog metadata and are not copied into Spark
    field metadata. Array elements and map values are nullable because Delta
    Engine declarations do not model their nullability separately.
    """
    try:
        from delta_engine.adapters.databricks.spark.schema import (
            to_spark_schema as _to_spark_schema,
        )
    except ModuleNotFoundError as error:
        if _is_pyspark_import_error(error):
            raise RuntimeError(_SPARK_RUNTIME_HINT) from error
        raise

    return _to_spark_schema(table.to_desired_table())


def build_sql_engine(connection: Connection) -> Engine:
    """
    Create an engine that syncs through a Databricks SQL warehouse connection.

    PySpark-free: pass a connection from ``databricks.sql.connect(...)``
    (the ``delta-engine[sql]`` extra) and syncs run from any plain Python
    environment. Unity Catalog only.
    """
    from delta_engine.adapters.databricks.warehouse.factory import build_engine as _build_engine

    return _build_engine(connection)


def build_reader(connection: Connection) -> CatalogStateReader:
    """
    Create a read-only catalog state reader for a Databricks SQL warehouse.

    The caller opens and owns the connection, exactly as for
    ``build_sql_engine``. The reader fetches one table's observed state and
    executes no DDL.
    """
    from delta_engine.adapters.databricks.warehouse.factory import build_reader as _build_reader

    return _build_reader(connection)


def configure_logging(level: int = logging.INFO, stream: TextIO | None = None) -> None:
    """Install the package's colored logging handler."""
    from delta_engine.adapters.databricks.log_config import (
        configure_logging as _configure_logging,
    )

    _configure_logging(level=level, stream=stream)
