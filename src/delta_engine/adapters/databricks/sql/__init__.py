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
from delta_engine.adapters.databricks.sql.features import enabled_features_from_properties
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
    Rows,
    RunQuery,
    read_column_tags,
    read_foreign_keys,
    read_primary_key,
    read_referencing_foreign_keys,
    read_table_tags,
    schema_exists,
)
from delta_engine.adapters.databricks.sql.types import render_data_type

__all__ = [
    "Rows",
    "RunQuery",
    "backtick",
    "backtick_qualified_name",
    "column_tags_query",
    "compile_plan",
    "describe_json_query",
    "enabled_features_from_properties",
    "foreign_keys_query",
    "primary_key_query",
    "quote_literal",
    "read_column_tags",
    "read_foreign_keys",
    "read_primary_key",
    "read_referencing_foreign_keys",
    "read_table_tags",
    "referencing_foreign_keys_query",
    "render_data_type",
    "schema_exists",
    "schema_exists_query",
    "table_tags_query",
]
