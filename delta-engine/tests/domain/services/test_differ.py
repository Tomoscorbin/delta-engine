from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.table import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
)
from delta_engine.domain.services.differ import diff_tables
from tests.factories import make_qualified_name


def test_returns_create_table_when_observed_is_missing() -> None:
    desired = DesiredTable(
        make_qualified_name("dev", "silver", "people"),
        (Column("id", Integer()), Column("name", String())),
    )

    plan = diff_tables(desired=desired, observed=None)

    assert isinstance(plan, ActionPlan)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateTable)


def test_no_changes_returns_empty_plan() -> None:
    columns = (Column("id", Integer()), Column("name", String()))
    observed = ObservedTable(make_qualified_name("dev", "silver", "people"), columns)
    desired = DesiredTable(make_qualified_name("dev", "silver", "people"), columns)

    plan = diff_tables(desired, observed)

    assert len(plan) == 0
    assert list(plan) == []
    assert not plan


def test_happy_path_produces_expected_actions() -> None:
    observed = ObservedTable(
        make_qualified_name("dev", "silver", "people"),
        (Column("id", Integer()), Column("nickname", String())),
    )
    desired = DesiredTable(
        make_qualified_name("dev", "silver", "people"),
        (Column("id", Integer()), Column("age", Integer())),
    )

    plan = diff_tables(desired, observed)

    assert tuple(plan) == (
        AddColumn(Column("age", Integer())),
        DropColumn("nickname"),
        SetColumnComment(column_name="age", comment=""),
    )
