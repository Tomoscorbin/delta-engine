"""Spark/Databricks SQL quoting & name rendering (single dialect)."""

from __future__ import annotations

from delta_engine.domain.model import QualifiedName


def quote_identifier(raw: str) -> str:
    """Backtick-quote an identifier part, escaping backticks."""
    return f"`{raw.replace('`', '``')}`"


def quote_literal(raw: str) -> str:
    """Single-quote a string literal, escaping single quotes."""
    return "'" + raw.replace("'", "''") + "'"


def quote_qualified_name(qualified_name: QualifiedName) -> str:
    """Render a fully qualified table name with dot separators and backticks."""
    parts = [
        qualified_name.catalog,
        qualified_name.schema,
        qualified_name.name,
    ]
    return ".".join(quote_identifier(p) for p in parts)
