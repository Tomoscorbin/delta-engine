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


def test_diff_tables_happy_path() -> None:
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


def test_diff_tables_returns_create_table_when_observed_is_none() -> None:
    desired = DesiredTable(
        make_qualified_name("dev", "silver", "people"),
        (Column("id", Integer()), Column("name", String())),
    )
    plan = diff_tables(desired=desired, observed=None)

    assert isinstance(plan, ActionPlan)
    assert isinstance(plan[0], CreateTable)


def test_diff_tables_no_changes_returns_empty_plan() -> None:
    cols = (Column("id", Integer()), Column("name", String()))
    observed = ObservedTable(make_qualified_name("dev", "silver", "people"), cols)
    desired = DesiredTable(make_qualified_name("dev", "silver", "people"), cols)
    plan = diff_tables(desired, observed)

    assert len(plan) == 0
    assert not plan
    assert list(plan) == []
