from __future__ import annotations
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True, slots=True)
class SparkSqlDialect:
    """
    Spark/Databricks SQL dialect.
    - Identifiers are backticked; internal backticks doubled.
    - String literals are single-quoted; internal single quotes doubled.
    - Qualified names are joined with '.'.
    """

    identifier_separator: str = "."

    def quote_identifier(self, raw: str) -> str:
        escaped = raw.replace("`", "``")
        return f"`{escaped}`"

    def quote_literal(self, raw: str) -> str:
        escaped = raw.replace("'", "''")
        return f"'{escaped}'"

    def join_qualified_name(self, parts: Sequence[str]) -> str:
        # Parts are expected to be already quoted.
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


SPARK_SQL = SparkSqlDialect()
