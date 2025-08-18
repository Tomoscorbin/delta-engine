from __future__ import annotations
from dataclasses import dataclass
from .base import SqlDialect

@dataclass(frozen=True)
class SparkSqlDialect:
    """Spark/Databricks SQL dialect.
    - Identifiers are backticked.
    - Backticks inside identifiers are escaped by doubling.
    - String literals are single-quoted; single quotes are escaped by doubling.
    - Name parts are joined with '.'.
    """
    identifier_separator: str = "."

    def quote_identifier(self, raw: str) -> str:
        escaped = raw.replace("`", "``")
        return f"`{escaped}`"

    def quote_literal(self, raw: str) -> str:
        escaped = raw.replace("'", "''")
        return f"'{escaped}'"

    def join_qualified_name(self, parts: list[str]) -> str:
        return self.identifier_separator.join(parts)

SPARK_SQL = SparkSqlDialect()