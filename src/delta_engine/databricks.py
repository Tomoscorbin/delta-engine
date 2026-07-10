"""
Public Databricks entry points.

The implementation lives in ``delta_engine.adapters.databricks``. This module
keeps the user-facing Databricks import path short while preserving lazy PySpark
imports for code that only declares schemas or inspects result types.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TextIO

from delta_engine.application.engine import Engine

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = ["build_engine", "configure_logging"]


def build_engine(spark: SparkSession) -> Engine:
    """Create an engine configured for Databricks."""
    from delta_engine.adapters.databricks.spark.factory import build_engine as _build_engine

    return _build_engine(spark)


def configure_logging(level: int = logging.INFO, stream: TextIO | None = None) -> None:
    """Install the package's colored logging handler."""
    from delta_engine.adapters.databricks.log_config import (
        configure_logging as _configure_logging,
    )

    _configure_logging(level=level, stream=stream)
