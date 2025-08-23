from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable


@runtime_checkable
class SqlDialect(Protocol):
    """Minimal contract every SQL dialect must satisfy."""

    def quote_identifier(self, raw: str) -> str:
        """Quote + escape a single identifier part (catalog / schema / table / column)."""

    def quote_literal(self, raw: str) -> str:
        """Quote + escape a string literal value (e.g., for TBLPROPERTIES, LOCATION)."""

    def join_qualified_name(self, parts: Sequence[str]) -> str:
        """Join already-quoted identifier parts with the dialect's separator (usually '.')."""

    def render_qualified_name(self, catalog: str | None, schema: str | None, name: str) -> str:
        """Render a fully qualified name with catalog, schema, and name."""
