from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Int64, String
from delta_engine.domain.services.column_diff import (
    diff_columns,
    diff_columns_for_adds,
    diff_columns_for_drops,
)
from tests.factories import columns


def test_diff_columns_for_adds_returns_missing_in_desired_order():
    desired = columns([("id", Int64()), ("name", String()), ("email", String()), ("age", Int64())])
    observed = columns([("id", Int64()), ("name", String())])

    actions = diff_columns_for_adds(desired, observed)

    assert tuple(a.column.name for a in actions) == ("email", "age")  # order = desired order
    assert all(isinstance(a.column, Column) for a in actions)


def test_diff_columns_for_drops_returns_missing_in_observed_order():
    desired = columns([("id", Int64())])
    observed = columns([("id", Int64()), ("name", String()), ("email", String())])

    actions = diff_columns_for_drops(desired, observed)

    assert tuple(a.name for a in actions) == ("name", "email")  # order = observed order


def test_diff_columns_combines_adds_then_drops():
    desired = columns([("id", Int64()), ("name", String())])
    observed = columns([("id", Int64()), ("email", String())])

    actions = diff_columns(desired, observed)

    # First adds (from desired order), then drops (from observed order)
    from delta_engine.domain.plan.actions import AddColumn, DropColumn
    assert isinstance(actions[0], AddColumn)
    assert actions[0].column.name == "name"
    assert isinstance(actions[1], DropColumn)
    assert actions[1].name == "email"
