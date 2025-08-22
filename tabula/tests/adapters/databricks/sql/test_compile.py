from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
import pytest

from tabula.adapters.databricks.sql import compile as compile_mod
from tabula.domain.plan.actions import CreateTable, AddColumn, DropColumn
from tabula.domain.model import Column, DataType
from tabula.adapters.databricks.sql.dialects.spark_sql import SPARK_SQL


# ------------------------- minimal plan double -------------------------------

@dataclass(frozen=True)
class _QualifiedName:
    catalog: str | None
    schema: str | None
    name: str


class _Plan:
    def __init__(self, qualified_name: _QualifiedName, actions: tuple[object, ...]) -> None:
       self.target = qualified_name
       self.actions = actions

    def __iter__(self):
        return iter(self.actions)


def _column(name: str, is_nullable: bool) -> Column:
    # Use real domain models; mapper will be monkeypatched
    dt = DataType("int") if name in {"id", "a", "b"} else DataType("string")
    return Column(name=name, data_type=dt, is_nullable=is_nullable)


# ------------------------------ tests ----------------------------------------

def test_empty_plan_returns_empty_tuple() -> None:
    plan = _Plan(_QualifiedName("c", "s", "t"), actions=())
    out = compile_mod.compile_plan(plan, dialect=SPARK_SQL)
    assert isinstance(out, tuple)
    assert out == ()


def test_create_table_respects_nullability_and_quotes(monkeypatch) -> None:
    # After fixing _column_definition to pass column.data_type, map by dt.name:
    monkeypatch.setattr(
        compile_mod,
        "sql_type_for_data_type",
        lambda dt: {"int": "INT", "string": "STRING"}[dt.name],
    )
    qn = _QualifiedName("c", "s", "t")
    actions = (CreateTable(columns=(_column("id", False), _column("note", True))),)
    plan = _Plan(qn, actions)

    out = compile_mod.compile_plan(plan, dialect=SPARK_SQL)

    assert out == (
        "CREATE TABLE IF NOT EXISTS `c`.`s`.`t` (`id` INT NOT NULL, `note` STRING NULL)",
    )


def test_add_and_drop_columns_are_one_to_one(monkeypatch) -> None:
    monkeypatch.setattr(compile_mod, "sql_type_for_data_type", lambda dt: "STRING")

    qn = _QualifiedName(None, "sales", "orders")
    actions = (
        AddColumn(column=_column("notes", is_nullable=True)),
        DropColumn(column_name="order"),  # check quoting of a reserved-ish name
    )
    plan = _Plan(qn, actions)

    out = compile_mod.compile_plan(plan, dialect=SPARK_SQL)

    assert out == (
        "ALTER TABLE `sales`.`orders` ADD COLUMN IF NOT EXISTS `notes` STRING NULL",
        "ALTER TABLE `sales`.`orders` DROP COLUMN IF EXISTS `order`",
    )


def test_action_order_is_preserved(monkeypatch) -> None:
    monkeypatch.setattr(compile_mod, "sql_type_for_data_type", lambda dt: "INT")

    qn = _QualifiedName("c", "s", "t")
    actions = (
        AddColumn(column=_column("a", is_nullable=False)),
        AddColumn(column=_column("b", is_nullable=False)),
        DropColumn(column_name="a"),
    )
    plan = _Plan(qn, actions)

    out = compile_mod.compile_plan(plan, dialect=SPARK_SQL)

    assert out == (
        "ALTER TABLE `c`.`s`.`t` ADD COLUMN IF NOT EXISTS `a` INT NOT NULL",
        "ALTER TABLE `c`.`s`.`t` ADD COLUMN IF NOT EXISTS `b` INT NOT NULL",
        "ALTER TABLE `c`.`s`.`t` DROP COLUMN IF EXISTS `a`",
    )


def test_unknown_action_raises_not_implemented() -> None:
    class RenameTable:
        pass

    qn = _QualifiedName("c", "s", "t")
    plan = _Plan(qn, (RenameTable(),))

    with pytest.raises(NotImplementedError, match="No SQL compiler for action RenameTable"):
        compile_mod.compile_plan(plan, dialect=SPARK_SQL)


def test_compiler_uses_injected_dialect_for_fqn_and_identifiers(monkeypatch) -> None:
    """
    Prove we are not hard-wired to Spark: inject a fake ANSI-ish dialect and
    ensure both the table FQN and column identifiers follow that dialect.
    """
    class _AnsiLikeDialect:
        def quote_identifier(self, raw: str) -> str:
            return '"' + raw.replace('"', '""') + '"'
        def render_qualified_name(self, catalog, schema, name) -> str:
            parts = [p for p in (catalog, schema, name) if p]
            return ".".join(self.quote_identifier(p) for p in parts)

    monkeypatch.setattr(compile_mod, "sql_type_for_data_type", lambda dt: "INT")

    qn = _QualifiedName("c", "s", "t")
    actions = (AddColumn(column=_column('weird"name', is_nullable=False)),)
    plan = _Plan(qn, actions)

    out = compile_mod.compile_plan(plan, dialect=_AnsiLikeDialect())

    assert out == (
        'ALTER TABLE "c"."s"."t" ADD COLUMN IF NOT EXISTS "weird""name" INT NOT NULL',
    )
