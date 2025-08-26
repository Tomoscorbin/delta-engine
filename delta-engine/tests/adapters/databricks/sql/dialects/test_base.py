from __future__ import annotations

from delta_engine.adapters.databricks.sql.dialects import SqlDialect
from delta_engine.adapters.databricks.sql.dialects.spark_sql import SPARK_SQL


def test_spark_sql_conforms_runtime() -> None:
    # Thanks to @runtime_checkable we can assert at runtime:
    assert isinstance(SPARK_SQL, SqlDialect)

    # Sanity: render_qualified_name quotes each part and joins with dots
    fqn = SPARK_SQL.render_qualified_name("cat", "sch", "tab")
    assert fqn == "`cat`.`sch`.`tab`"

    # Identifiers/literals are distinct concerns
    assert SPARK_SQL.quote_identifier("a`b") == "`a``b`"
    assert SPARK_SQL.quote_literal("x'y") == "'x''y'"


def test_custom_dialect_example_conforms_and_behaves() -> None:
    # Minimal ANSI-like dialect to prove the protocol shape and intended behavior.
    class AnsiLikeDialect:
        def quote_identifier(self, raw: str) -> str:
            return '"' + raw.replace('"', '""') + '"'

        def quote_literal(self, raw: str) -> str:
            return "'" + raw.replace("'", "''") + "'"

        def join_qualified_name(self, parts: list[str]) -> str:
            return ".".join(parts)

        def render_qualified_name(self, catalog: str | None, schema: str | None, name: str) -> str:
            parts = [p for p in (catalog, schema, name) if p]
            return self.join_qualified_name([self.quote_identifier(p) for p in parts])

    d = AnsiLikeDialect()
    assert isinstance(d, SqlDialect)

    # FQN quoting + joining is as expected
    assert d.render_qualified_name("c", "s", "t") == '"c"."s"."t"'
    # Escaping checks
    assert d.quote_identifier('weird"name') == '"weird""name"'
    assert d.quote_literal("O'Hara") == "'O''Hara'"
