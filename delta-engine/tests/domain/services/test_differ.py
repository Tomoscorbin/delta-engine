import pytest

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.table import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import ActionPlan, AddColumn, CreateTable, DropColumn
from delta_engine.domain.services.differ import diff_tables
from tests.factories import make_qualified_name


def test_diff_tables_returns_create_table_when_observed_is_none() -> None:
    desired = DesiredTable(
        make_qualified_name("dev", "silver", "people"),
        (Column("id", Integer()), Column("name", String())),
    )
    plan = diff_tables(observed=None, desired=desired)

    assert isinstance(plan, ActionPlan)
    assert isinstance(plan[0], CreateTable)


def test_diff_tables_raises_on_qualified_name_mismatch() -> None:
    observed = ObservedTable(
        make_qualified_name("dev", "silver", "people_old"), (Column("id", Integer()),)
    )
    desired = DesiredTable(
        make_qualified_name("dev", "silver", "people_new"), (Column("id", Integer()),)
    )
    with pytest.raises(ValueError):
        diff_tables(observed, desired)


def test_diff_tables_produces_adds_and_drops_from_column_diff() -> None:
    observed = ObservedTable(
        make_qualified_name("dev", "silver", "people"),
        (Column("id", Integer()), Column("nickname", String())),
    )
    desired = DesiredTable(
        make_qualified_name("dev", "silver", "people"),
        (Column("id", Integer()), Column("age", Integer())),
    )
    plan = diff_tables(observed, desired)

    assert tuple(plan) == (
        AddColumn(Column("age", Integer())),
        DropColumn("nickname"),
    )


def test_diff_tables_no_changes_returns_empty_plan() -> None:
    cols = (Column("id", Integer()), Column("name", String()))
    observed = ObservedTable(make_qualified_name("dev", "silver", "people"), cols)
    desired = DesiredTable(make_qualified_name("dev", "silver", "people"), cols)
    plan = diff_tables(observed, desired)

    assert len(plan) == 0
    assert not plan
    assert list(plan) == []
