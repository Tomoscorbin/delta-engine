"""Spark SQL dialect implementation."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SparkSqlDialect:
    """Spark/Databricks SQL dialect."""

    identifier_separator: str = "."

    def quote_identifier(self, raw: str) -> str:
        """Return a backticked identifier."""

        escaped = raw.replace("`", "``")
        return f"`{escaped}`"

    def quote_literal(self, raw: str) -> str:
        """Return a single-quoted string literal."""

        escaped = raw.replace("'", "''")
        return f"'{escaped}'"

    def join_qualified_name(self, parts: Sequence[str]) -> str:
        """Join already quoted identifier parts."""

        return self.identifier_separator.join(parts)

    def render_qualified_name(
        self,
        catalog: str | None,
        schema: str | None,
        name: str,
    ) -> str:
        """Render a fully qualified table name."""

        parts = [p for p in (catalog, schema, name) if p]
        quoted = [self.quote_identifier(p) for p in parts]
        return self.join_qualified_name(quoted)


SPARK_SQL = SparkSqlDialect()
