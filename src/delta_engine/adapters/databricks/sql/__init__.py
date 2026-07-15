"""
SQL generation and type rendering for the Databricks adapter.

This package's adapter-internal surface is re-exported here: callers such as
``reader`` and ``executor`` import from ``delta_engine.adapters.databricks.sql``
and never need to know which internal module a name lives in. Internal modules
import from each other directly (not through this ``__init__``) to avoid import
cycles.
"""

from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.adapters.databricks.sql.describe_json import parse_described_table
from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.adapters.databricks.sql.parse import parse_data_type
from delta_engine.adapters.databricks.sql.queries import (
    column_tags_query,
    describe_detail_query,
    describe_json_query,
    foreign_keys_query,
    information_schema_probe_query,
    primary_key_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.sql.rows import (
    clustering_columns_from_detail_row,
    column_from_catalog,
    column_tags_from_rows,
    foreign_keys_from_rows,
    managed_properties_from_detail_row,
    primary_key_from_rows,
    referencing_foreign_keys_from_rows,
    table_tags_from_rows,
)
from delta_engine.adapters.databricks.sql.types import render_data_type

__all__ = [
    "backtick",
    "backtick_qualified_name",
    "clustering_columns_from_detail_row",
    "column_from_catalog",
    "column_tags_from_rows",
    "column_tags_query",
    "compile_plan",
    "describe_detail_query",
    "describe_json_query",
    "foreign_keys_from_rows",
    "foreign_keys_query",
    "information_schema_probe_query",
    "managed_properties_from_detail_row",
    "parse_data_type",
    "parse_described_table",
    "primary_key_from_rows",
    "primary_key_query",
    "quote_literal",
    "referencing_foreign_keys_from_rows",
    "referencing_foreign_keys_query",
    "render_data_type",
    "table_tags_from_rows",
    "table_tags_query",
]
