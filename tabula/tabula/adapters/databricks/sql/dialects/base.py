from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Optional, Sequence

class SqlDialect(Protocol):
    """Minimal contract every SQL dialect must satisfy."""

    def quote_identifier(self, raw: str) -> str:
        """Quote + escape a single identifier part (catalog / schema / table / column)."""

    def quote_literal(self, raw: str) -> str:
        """Quote + escape a string literal value (e.g., for TBLPROPERTIES, LOCATION)."""

    def join_qualified_name(self, parts: Sequence[str]) -> str:
        """Join already-quoted identifier parts with the dialect's separator (usually '.')."""

# Helper: engine-agnostic rendering functions that callers can import.
def render_fully_qualified_name(
    catalog: Optional[str],
    schema: Optional[str],
    name: str,
    *,
    dialect: SqlDialect,
) -> str:
    """Quote each non-empty part and join with the dialect's separator."""
    parts: list[str] = []
    if catalog:
        parts.append(dialect.quote_identifier(catalog))
    if schema:
        parts.append(dialect.quote_identifier(schema))
    parts.append(dialect.quote_identifier(name))
    return dialect.join_qualified_name(parts)

def quote_property_kv(
    properties: dict[str, str] | None,
    *,
    dialect: SqlDialect,
) -> str:
    """Render a TBLPROPERTIES(...) list. Returns empty string when no properties."""
    if not properties:
        return ""
    items = [
        f"{dialect.quote_literal(k)} = {dialect.quote_literal(v)}"
        for k, v in properties.items()
    ]
    return ", ".join(items)
