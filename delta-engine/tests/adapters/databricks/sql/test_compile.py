import pytest

from delta_engine.adapters.databricks.sql.compile import _compile_action, compile_plan
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Array, Decimal, Integer, Map, String
from delta_engine.domain.plan.actions import Action, ActionPlan, AddColumn, CreateTable, DropColumn
from tests.factories import make_qualified_name

_QN = make_qualified_name("dev", "silver", "people")


def test_compile_create_table_with_nullable_and_nonnullable_columns() -> None:
    plan = ActionPlan(
        target=_QN,
        actions=(
            CreateTable(
                columns=(
                    Column("id", Integer(), is_nullable=False),
                    Column("name", String()),  # default nullable
                )
            ),
        ),
    )
    (sql,) = compile_plan(plan)
    assert sql == (
        "CREATE TABLE IF NOT EXISTS `dev`.`silver`.`people` (`id` INT NOT NULL, `name` STRING)"
    )


def test_compile_add_and_drop_columns_and_quoting() -> None:
    plan = ActionPlan(
        target=_QN,
        actions=(
            AddColumn(Column("age", Integer(), is_nullable=False)),
            # column name contains backtick -> must be doubled inside quotes
            DropColumn("nick`name"),
        ),
    )
    sqls = compile_plan(plan)
    assert sqls == (
        "ALTER TABLE `dev`.`silver`.`people` ADD COLUMN `age` INT NOT NULL",
        "ALTER TABLE `dev`.`silver`.`people` DROP COLUMN `nick``name`",
    )


def test_column_def_no_trailing_space_when_nullable_true() -> None:
    plan = ActionPlan(
        target=_QN,
        actions=(AddColumn(Column("nickname", String(), is_nullable=True)),),
    )
    (sql,) = compile_plan(plan)
    # ends with STRING, not "STRING "
    assert sql.endswith("`nickname` STRING")


def test_create_table_with_complex_types_decimal_array_map() -> None:
    plan = ActionPlan(
        target=_QN,
        actions=(
            CreateTable(
                columns=(
                    Column("amount", Decimal(10, 2), is_nullable=False),
                    Column("tags", Array(String())),
                    Column("attrs", Map(String(), Integer())),
                )
            ),
        ),
    )
    (sql,) = compile_plan(plan)
    # Spot-check the type renderings
    assert "DECIMAL(10,2)" in sql
    assert "ARRAY<STRING>" in sql
    assert "MAP<STRING,INT>" in sql


def test_unknown_action_raises_from_singledispatch() -> None:
    class RenameColumn(Action):
        pass

    with pytest.raises(NotImplementedError) as exc:
        _compile_action(RenameColumn(), "`dev`.`silver`.`people`")
    assert "RenameColumn" in str(exc.value)
