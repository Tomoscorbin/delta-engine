from dataclasses import FrozenInstanceError

import pytest

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.plan.actions import ActionPlan, AddColumn, CreateTable, DropColumn
from tests.factories import make_qualified_name

_QN = make_qualified_name("dev", "silver", "orders")


def test_action_plan_len_bool_iter_and_indexing() -> None:
    actions = (
        AddColumn(Column("age", Integer())),
        DropColumn("nickname"),
    )
    plan = ActionPlan(target=_QN, actions=actions)

    assert len(plan) == 2
    assert bool(plan) is True
    assert list(iter(plan)) == list(actions)
    assert plan[0] == actions[0]
    assert plan[0:2] == actions


def test_action_plan_empty_is_falsey() -> None:
    plan = ActionPlan(target=_QN, actions=())
    assert len(plan) == 0
    assert bool(plan) is False
    assert list(plan) == []


def test_action_plan_is_frozen() -> None:
    plan = ActionPlan(target=_QN, actions=())
    with pytest.raises(FrozenInstanceError):
        plan.target = _QN


def test_action_classes_are_value_objects() -> None:
    c = CreateTable(columns=(Column("id", Integer()), Column("name", String())))
    assert isinstance(c.columns, tuple)
    assert c.columns[0].name == "id"
