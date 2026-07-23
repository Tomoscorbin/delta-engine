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

if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from pyspark.sql import SparkSession

__all__ = ["build_spark_engine", "build_sql_engine", "configure_logging"]

_SPARK_RUNTIME_HINT = (
    "delta-engine's Spark backend requires the PySpark supplied by a supported "
    "Databricks Runtime."
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


def build_sql_engine(connection: Connection) -> Engine:
    """
    Create an engine that syncs through a Databricks SQL warehouse connection.

    PySpark-free: pass a connection from ``databricks.sql.connect(...)``
    (the ``delta-engine[sql]`` extra) and syncs run from any plain Python
    environment. Unity Catalog only.
    """
    from delta_engine.adapters.databricks.warehouse.factory import build_engine as _build_engine

    return _build_engine(connection)


def configure_logging(level: int = logging.INFO, stream: TextIO | None = None) -> None:
    """Install the package's colored logging handler."""
    from delta_engine.adapters.databricks.log_config import (
        configure_logging as _configure_logging,
    )

    _configure_logging(level=level, stream=stream)
