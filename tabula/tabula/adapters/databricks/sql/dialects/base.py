"""Protocol for SQL dialect implementations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable


@runtime_checkable
class SqlDialect(Protocol):
    """Minimal contract every SQL dialect must satisfy."""

    def quote_identifier(self, raw: str) -> str:
        """Quote and escape a single identifier part."""

    def quote_literal(self, raw: str) -> str:
        """Quote and escape a string literal value."""

    def join_qualified_name(self, parts: Sequence[str]) -> str:
        """Join quoted identifier parts with the dialect's separator."""

    def render_qualified_name(self, catalog: str | None, schema: str | None, name: str) -> str:
        """Render a fully qualified name from its components."""
