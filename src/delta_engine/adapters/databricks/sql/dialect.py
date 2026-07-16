"""Spark/Databricks SQL quoting & name rendering."""

from delta_engine.domain.model import QualifiedName


def backtick(raw: str) -> str:
    """Backtick-quote an identifier part, doubling any embedded backtick."""
    return f"`{raw.replace('`', '``')}`"


def quote_literal(literal: str) -> str:
    """Single-quote a string literal, escaping backslashes and single quotes."""
    escaped = literal.replace("\\", "\\\\").replace("'", "''")
    return "'" + escaped + "'"


def backtick_qualified_name(qualified_name: QualifiedName) -> str:
    """Render a QualifiedName as backtick-quoted, dot-separated parts."""
    return ".".join(backtick(p) for p in qualified_name.parts)
