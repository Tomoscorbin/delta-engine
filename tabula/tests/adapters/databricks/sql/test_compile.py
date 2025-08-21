from __future__ import annotations

from types import SimpleNamespace
import pytest
from tabula.adapters.databricks.sql import compile as compile_mod
from tabula.domain.plan.actions import CreateTable, AddColumn, DropColumn
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.adapters.databricks.sql.dialects.spark_sql import SPARK_SQL


# ------------------------- minimal test doubles ------------------------------

class _QualifiedName:
    def __init__(self, catalog: str | None, schema: str | None, name: str) -> None:
        self.catalog = catalog
        self.schema = schema
        self.name = name


class _Plan:
    """Duck-typed ActionPlan: holds qualified_name and is iterable over actions."""
    def __init__(self, qualified_name: _QualifiedName, actions: tuple[object, ...]) -> None:
        self.qualified_name = qualified_name
        self._actions = actions

    def __iter__(self):
        return iter(self._actions)


def _column(name: str, *, is_nullable: bool) -> Column:
    """
    Construct a real domain Column. Data type does not matter for these tests
    because sql_type_for_data_type is monkeypatched; pass a dummy object.
    """
    dt = DataType("int") if name in {"id", "a", "b"} else DataType("string")
    return Column(name=name, data_type=dt, is_nullable=is_nullable)

# ------------------------------ tests ----------------------------------------

def test_create_table_uses_delta_and_respects_nullability(monkeypatch) -> None:
    # Deterministic type mapping
    monkeypatch.setattr(
        compile_mod,
        "sql_type_for_data_type",
        lambda col: {"id": "INT", "note": "STRING"}[col.name],
    )

    qualified_name = _QualifiedName("c", "s", "t")
    actions = (
        CreateTable(columns=(_column("id", is_nullable=False), _column("note", is_nullable=True))),
    )
    plan = _Plan(qualified_name, actions)

    statements_sql = compile_mod.compile_plan(plan, dialect=SPARK_SQL)

    assert isinstance(statements_sql, tuple)
    assert statements_sql == (
        "CREATE TABLE IF NOT EXISTS `c`.`s`.`t` (`id` INT NOT NULL, `note` STRING NULL)",
    )


def test_add_and_drop_columns_are_one_to_one(monkeypatch) -> None:
    monkeypatch.setattr(compile_mod, "sql_type_for_data_type", lambda col: "STRING")

    qualified_name = _QualifiedName(None, "sales", "orders")
    actions = (
        AddColumn(column=_column("notes", is_nullable=True)),
        DropColumn(column_name="order"),  # exercise quoting on a reserved-ish name
    )
    plan = _Plan(qualified_name, actions)

    statements_sql = compile_mod.compile_plan(plan, dialect=SPARK_SQL)

    assert statements_sql == (
        "ALTER TABLE `sales`.`orders` ADD COLUMN IF NOT EXISTS `notes` STRING NULL",
        "ALTER TABLE `sales`.`orders` DROP COLUMN IF EXISTS `order`",
    )


def test_action_order_is_preserved(monkeypatch) -> None:
    monkeypatch.setattr(compile_mod, "sql_type_for_data_type", lambda col: "INT")

    qualified_name = _QualifiedName("c", "s", "t")
    actions = (
        AddColumn(column=_column("a", is_nullable=False)),
        AddColumn(column=_column("b", is_nullable=False)),
        DropColumn(column_name="a"),
    )
    plan = _Plan(qualified_name, actions)

    statements_sql = compile_mod.compile_plan(plan, dialect=SPARK_SQL)

    assert statements_sql == (
        "ALTER TABLE `c`.`s`.`t` ADD COLUMN IF NOT EXISTS `a` INT NOT NULL",
        "ALTER TABLE `c`.`s`.`t` ADD COLUMN IF NOT EXISTS `b` INT NOT NULL",
        "ALTER TABLE `c`.`s`.`t` DROP COLUMN IF EXISTS `a`",
    )


def test_unknown_action_raises_not_implemented() -> None:
    class RenameTable:
        pass

    qualified_name = _QualifiedName("c", "s", "t")
    plan = _Plan(qualified_name, (RenameTable(),))

    with pytest.raises(NotImplementedError):
        compile_mod.compile_plan(plan, dialect=SPARK_SQL)


def test_compiler_uses_injected_dialect_for_fqn_and_identifiers(monkeypatch) -> None:
    """
    Prove we are not hard-wired to Spark: inject a fake ANSI-ish dialect and
    ensure both the table FQN and column identifiers follow that dialect.
    """
    class _AnsiLikeDialect:
        def quote_identifier(self, raw: str) -> str:
            return '"' + raw.replace('"', '""') + '"'

        def quote_literal(self, raw: str) -> str:
            return "'" + raw.replace("'", "''") + "'"

        def join_qualified_name(self, parts) -> str:
            return ".".join(parts)

        def render_qualified_name(self, catalog, schema, name) -> str:
            parts = [p for p in (catalog, schema, name) if p]
            return self.join_qualified_name([self.quote_identifier(p) for p in parts])

    monkeypatch.setattr(compile_mod, "sql_type_for_data_type", lambda col: "INT")

    qualified_name = _QualifiedName("c", "s", "t")
    actions = (AddColumn(column=_column('weird"name', is_nullable=False)),)
    plan = _Plan(qualified_name, actions)

    statements_sql = compile_mod.compile_plan(plan, dialect=_AnsiLikeDialect())

    assert statements_sql == (
        'ALTER TABLE "c"."s"."t" ADD COLUMN IF NOT EXISTS "weird""name" INT NOT NULL',
    )
