"""
SQL generation and type rendering for the Databricks adapter.

This package's adapter-internal surface is re-exported here: callers such as
``reader`` and ``executor`` import from ``delta_engine.adapters.databricks.sql``
and never need to know which internal module a name lives in. Internal modules
import from each other directly (not through this ``__init__``) to avoid import
cycles.
"""

from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.adapters.databricks.sql.queries import (
    column_tags_query,
    describe_json_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    schema_exists_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.sql.rows import (
    column_tags_from_rows,
    foreign_keys_from_rows,
    primary_key_from_rows,
    referencing_foreign_keys_from_rows,
    table_tags_from_rows,
)
from delta_engine.adapters.databricks.sql.types import render_data_type

__all__ = [
    "backtick",
    "backtick_qualified_name",
    "column_tags_from_rows",
    "column_tags_query",
    "compile_plan",
    "describe_json_query",
    "foreign_keys_from_rows",
    "foreign_keys_query",
    "primary_key_from_rows",
    "primary_key_query",
    "quote_literal",
    "referencing_foreign_keys_from_rows",
    "referencing_foreign_keys_query",
    "render_data_type",
    "schema_exists_query",
    "table_tags_from_rows",
    "table_tags_query",
]
