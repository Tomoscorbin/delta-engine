"""Spark/Databricks SQL quoting & name rendering."""

from __future__ import annotations

from delta_engine.domain.model import QualifiedName


def quote_identifier(raw: str) -> str:
    """Backtick-quote an identifier part, escaping backticks."""
    return f"`{raw.replace('`', '``')}`"


def quote_literal(raw: str) -> str:
    """Single-quote a string literal, escaping single quotes."""
    return "'" + raw.replace("'", "''") + "'"


def quote_qualified_name(qualified_name: QualifiedName) -> str:  # TODO: rename to backtick
    """Render a fully qualified table name with dot separators and backticks."""
    return ".".join(quote_identifier(p) for p in qualified_name.parts)
