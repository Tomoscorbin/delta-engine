
import pytest

from tabula.domain.model.column import Column
from tabula.domain.model.data_type import String
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.plan.actions import ActionPlan, AddColumn, CreateTable, DropColumn
from tests.factories import qualified_name

_QN = qualified_name("core", "gold", "customers")

def test_action_plan_iter_len_and_add():
    c1 = AddColumn(column=Column("name", String()))
    c2 = DropColumn(name="old_field")

    plan = ActionPlan(target=_QN, actions=(c1,))
    assert list(iter(plan)) == [c1]
    assert len(plan) == 1

    plan2 = plan.add(c2)
    assert len(plan2) == 2
    assert plan2.actions == (c1, c2)


def test_action_plan_add_requires_action_type():
    plan = ActionPlan(target=_QN, actions=())
    with pytest.raises(TypeError):
        plan.add("not-an-action")  # type: ignore[arg-type]


def test_action_plan_merge_concatenates_when_same_target():
    a1 = AddColumn(column=Column("name", String()))
    a2 = DropColumn(name="age")

    p1 = ActionPlan(target=_QN, actions=(a1,))
    p2 = ActionPlan(target=_QN, actions=(a2,))

    merged = p1 + p2
    assert merged.actions == (a1, a2)
    assert str(merged.target) == "core.gold.customers"


def test_action_plan_merge_rejects_different_targets():
    p1 = ActionPlan(target=QualifiedName("core", "gold", "a"), actions=())
    p2 = ActionPlan(target=QualifiedName("core", "gold", "b"), actions=())
    with pytest.raises(ValueError):
        _ = p1 + p2


def test_count_by_action_returns_counts_keyed_by_type():
    a1 = AddColumn(column=Column("name", String()))
    a2 = DropColumn(name="age")
    a3 = DropColumn(name="email")

    plan = ActionPlan(target=_QN, actions=(a1, a2, a3))
    counts = plan.count_by_action()

    # Keys are classes, values are counts
    assert counts.get(AddColumn) == 1
    assert counts.get(DropColumn) == 2
    assert counts.get(CreateTable, 0) == 0
