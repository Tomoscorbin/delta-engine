"""Spark/Databricks SQL quoting & name rendering (single dialect)."""

from __future__ import annotations


def quote_identifier(raw: str) -> str:
    """Backtick-quote an identifier part, escaping backticks."""
    return f"`{raw.replace('`', '``')}`"


def quote_literal(raw: str) -> str:
    """Single-quote a string literal, escaping single quotes."""
    return "'" + raw.replace("'", "''") + "'"


def render_qualified_name(catalog: str, schema: str, name: str) -> str:
    """Render a fully qualified table name with dot separators and backticks."""
    parts = [p for p in (catalog, schema, name) if p]
    return ".".join(quote_identifier(p) for p in parts)
