"""
SQL generation and type mapping for the Databricks adapter.

This package's full surface is re-exported here: callers (`reader`, `executor`,
`build_engine`) import from ``delta_engine.adapters.databricks.sql`` and never
need to know which internal module a name lives in. Internal modules import from
each other directly (not through this ``__init__``) to avoid import cycles.
"""

from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.adapters.databricks.sql.preview import error_preview, sql_preview
from delta_engine.adapters.databricks.sql.types import (
    domain_type_from_spark,
    sql_type_for_data_type,
)

__all__ = [
    "backtick",
    "backtick_qualified_name",
    "compile_plan",
    "domain_type_from_spark",
    "error_preview",
    "quote_literal",
    "sql_preview",
    "sql_type_for_data_type",
]
