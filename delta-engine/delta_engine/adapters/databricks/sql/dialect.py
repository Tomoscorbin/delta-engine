"""Spark/Databricks SQL quoting & name rendering."""

from __future__ import annotations

from delta_engine.domain.model import QualifiedName


def backtick(raw: str) -> str:
    """Backtick-quote an identifier part, escaping backtick_qualified_names."""
    return f"`{raw.replace('`', '``')}`"


def quote_literal(literal: str) -> str:
    """Single-quote a string literal, escaping single quotes."""
    return "'" + literal.replace("'", "''") + "'"


def backtick_qualified_name(qualified_name: QualifiedName) -> str:
    """Render a fully qualified table name with dot separators and backtick_qualified_names."""
    return ".".join(backtick(p) for p in qualified_name.parts)
