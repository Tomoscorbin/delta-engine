from delta_engine.adapters.databricks.sql.dialect import (
    quote_identifier,
    quote_literal,
    quote_qualified_name,
)
from delta_engine.domain.model import QualifiedName


def query_describe_detail(qualified_name: QualifiedName) -> str:
    """Return SQL to read table details."""
    return f"DESCRIBE DETAIL {quote_qualified_name(qualified_name)}".strip()

def query_table_existence(qualified_name: QualifiedName) -> str:
    """Return SQL that checks if a table exists in the given catalog/schema."""
    tables_fqn = ".".join([
        quote_identifier(qualified_name.catalog),
        quote_identifier("information_schema"),
        quote_identifier("tables"),
    ])

    return f"""
        SELECT 1
        FROM {tables_fqn}
        WHERE table_schema = {quote_literal(qualified_name.schema)}
        AND table_name   = {quote_literal(qualified_name.name)}
        LIMIT 1
        """.strip()
