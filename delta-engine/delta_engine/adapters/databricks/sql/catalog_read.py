from delta_engine.adapters.databricks.sql.dialect import quote_qualified_name


def query_table_properties(fully_qualified_name: str) -> str:
    """Return SQL to read table properties (optionally a single key)."""
    return f"SHOW TBLPROPERTIES {quote_qualified_name(fully_qualified_name)}"
