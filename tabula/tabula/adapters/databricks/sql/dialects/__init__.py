"""Supported SQL dialects for compilation."""

from tabula.adapters.databricks.sql.dialects.base import SqlDialect
from tabula.adapters.databricks.sql.dialects.spark_sql import SPARK_SQL

__all__ = ["SPARK_SQL", "SqlDialect"]

