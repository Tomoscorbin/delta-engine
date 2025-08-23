from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AnsiSqlDialect:
    """ANSI-ish SQL dialect: double-quoted identifiers, single-quoted literals."""

    identifier_separator: str = "."

    def quote_identifier(self, raw: str) -> str:
        # Double any internal double quotes.
        escaped = raw.replace('"', '""')
        return f'"{escaped}"'

    def quote_literal(self, raw: str) -> str:
        # Double any internal single quotes.
        escaped = raw.replace("'", "''")
        return f"'{escaped}'"

    def join_qualified_name(self, parts: Sequence[str]) -> str:
        # Parts are assumed to be already quoted/escaped.
        return self.identifier_separator.join(parts)

    def render_qualified_name(
        self,
        catalog: str | None,
        schema: str | None,
        name: str,
    ) -> str:
        parts = [p for p in (catalog, schema, name) if p]
        quoted = [self.quote_identifier(p) for p in parts]
        return self.join_qualified_name(quoted)


ANSI_SQL = AnsiSqlDialect()
