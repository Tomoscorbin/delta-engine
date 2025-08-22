from __future__ import annotations

import pytest

from tabula.domain.plan.actions import (
    Action, ActionPlan, AddColumn, CreateTable, DropColumn,
)
from tabula.domain.model.column import Column
from tabula.domain.model.types import integer
from tabula.domain.model.qualified_name import QualifiedName


# ---------------------------
# Base / isinstance
# ---------------------------

def test_action_isinstance() -> None:
    a = AddColumn(Column("x", integer()))
    d = DropColumn("x")
    c = CreateTable((Column("a", integer()),))
    for act in (a, d, c):
        assert isinstance(act, Action)


# ---------------------------
# AddColumn
# ---------------------------

def test_addcolumn_requires_column_type() -> None:
    with pytest.raises(TypeError):
        AddColumn("id")  # type: ignore[arg-type]

def test_addcolumn_normalizes_nested_column_name() -> None:
    a = AddColumn(Column("UserID", integer()))
    assert a.column.name == "userid"


# ---------------------------
# DropColumn
# ---------------------------

def test_drop_column_name_normalized() -> None:
    d = DropColumn("UserID")
    assert d.column_name == "userid"

@pytest.mark.parametrize("bad", ["", " ", " id", "id ", "first name", "user.id"])
def test_drop_column_rejects_invalid_names(bad: str) -> None:
    with pytest.raises(ValueError):
        DropColumn(bad)

def test_drop_column_rejects_non_string() -> None:
    with pytest.raises(TypeError):
        DropColumn(123)  # type: ignore[arg-type]


# ---------------------------
# CreateTable (thin action: only shape enforced here)
# ---------------------------

def test_create_table_requires_tuple_columns() -> None:
    with pytest.raises(TypeError):
        CreateTable([Column("a", integer()), Column("b", integer())])  # type: ignore[arg-type]

def test_create_table_preserves_columns_order_and_identity() -> None:
    c1 = Column("A", integer())
    c2 = Column("B", integer())
    ct = CreateTable((c1, c2))
    assert ct.columns == (c1, c2)           # order + identity preserved
    assert ct.columns is not (c1, c2)       # different tuple object, but
    assert ct.columns[0] is c1 and ct.columns[1] is c2


# ---------------------------
# ActionPlan: container behavior
# ---------------------------

def make_qn(c: str = "c", s: str = "s", n: str = "t") -> QualifiedName:
    return QualifiedName(c, s, n)

def test_len_empty_action_plan() -> None:
    plan = ActionPlan(make_qn())
    assert len(plan) == 0  # empty plan has no actions

def test_container_truthiness_and_iteration() -> None:
    plan = ActionPlan(make_qn())
    assert not plan  # empty -> False
    plan2 = plan.add(AddColumn(Column("a", integer())))
    assert plan2      # non-empty -> True
    assert [type(a).__name__ for a in plan2] == ["AddColumn"]

def test_action_plan_add_returns_new_instance_and_preserves_order() -> None:
    plan = ActionPlan(make_qn())
    a1 = AddColumn(Column("A", integer()))
    a2 = AddColumn(Column("B", integer()))
    plan2 = plan.add(a1)
    plan3 = plan2.add(a2)
    assert [a is b for a, b in zip(plan3, (a1, a2))] == [True, True]  # identity preserved
    assert plan is not plan2 and plan2 is not plan3  # immutability

def test_action_plan_add_rejects_non_action() -> None:
    plan = ActionPlan(make_qn())
    with pytest.raises(TypeError):
        plan.add("not-an-action")  # type: ignore[arg-type]


# ---------------------------
# ActionPlan: merge semantics
# ---------------------------

def test_extend_requires_same_target() -> None:
    p1 = ActionPlan(make_qn("c1", "s", "t"))
    p2 = ActionPlan(make_qn("c2", "s", "t"))
    with pytest.raises(ValueError):
        _ = p1 + p2

def test_extend_with_same_target_concatenates_and_is_immutable() -> None:
    a1 = AddColumn(Column("a", integer()))
    a2 = AddColumn(Column("b", integer()))
    p1 = ActionPlan(make_qn("c", "s", "t")).add(a1)
    p2 = ActionPlan(make_qn("c", "s", "t")).add(a2)
    p3 = p1 + p2
    assert [type(a).__name__ for a in p3] == ["AddColumn", "AddColumn"]
    assert p3 is not p1 and p3 is not p2
    # ensure actions are the same objects (not copied/rewrapped)
    assert [a is b for a, b in zip(p3, (a1, a2))] == [True, True]


# ---------------------------
# ActionPlan: counting by type
# ---------------------------

def test_counts_by_action_type() -> None:
    plan = ActionPlan(make_qn())
    plan = plan.add(AddColumn(Column("a", integer())))
    plan = plan.add(DropColumn("b"))
    plan = plan.add(AddColumn(Column("c", integer())))

    counts = plan.count_by_action()
    assert counts[AddColumn] == 2
    assert counts[DropColumn] == 1
    assert counts.get(CreateTable, 0) == 0
    assert sum(counts.values()) == len(tuple(plan))
