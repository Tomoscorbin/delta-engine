from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class AnsiSqlDialect:
    identifier_separator: str = "."

    def quote_identifier(self, raw: str) -> str:
        # Double the double quotes inside identifiers
        escaped = raw.replace('"', '""')
        return f'"{escaped}"'

    def quote_literal(self, raw: str) -> str:
        escaped = raw.replace("'", "''")
        return f"'{escaped}'"

    def join_qualified_name(self, parts: list[str]) -> str:
        return self.identifier_separator.join(parts)
