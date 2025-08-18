import pytest

from tabula.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
)
from tabula.domain.model.column import Column
from tabula.domain.model.types import integer


# ---------------------------
# Base / isinstance / labels
# ---------------------------

def test_action_isinstance():
    a = AddColumn(Column("x", integer()))
    d = DropColumn("x")
    c = CreateTable((Column("a", integer()),))
    for act in (a, d, c):
        assert isinstance(act, Action)


# ---------------------------
# AddColumn
# ---------------------------

def test_addcolumn_requires_column_type():
    with pytest.raises(TypeError):
        AddColumn("id")  # type: ignore[arg-type]


def test_addcolumn_str_uses_normalized_column_name():
    a = AddColumn(Column("UserID", integer()))
    assert str(a) == "add column userid"


# ---------------------------
# DropColumn
# ---------------------------

def test_drop_column_name_normalized_and_in_str():
    d = DropColumn("UserID")
    assert d.column_name == "userid"
    assert str(d) == "drop column userid"


@pytest.mark.parametrize("bad", ["", " ", " id", "id ", "first name", "user.id"])
def test_drop_column_rejects_invalid_names(bad):
    with pytest.raises(ValueError):
        DropColumn(bad)


def test_drop_column_rejects_non_string():
    with pytest.raises(TypeError):
        DropColumn(123)  # type: ignore[arg-type]


# ---------------------------
# CreateTable (thin action: only shape enforced here)
# ---------------------------

def test_create_table_requires_tuple_columns():
    with pytest.raises(TypeError):
        CreateTable([Column("a", integer()), Column("b", integer())])  # type: ignore[arg-type]


def test_create_table_string_form_mentions_count():
    c = CreateTable((Column("A", integer()), Column("B", integer())))
    s = str(c).lower()
    assert "create table" in s and "2 column" in s


# ---------------------------
# ActionPlan: container behavior
# ---------------------------

def test_len_empty_action_plan(make_qn):
    plan = ActionPlan(make_qn())
    assert len(plan) == 0  # empty plan has no actions

def test_container_niceties_and_truthiness(make_qn):
    plan = ActionPlan(make_qn())
    assert not plan  # empty -> False
    plan2 = plan.add(AddColumn(Column("a", integer())))
    assert plan2      # non-empty -> True
    assert len(tuple(iter(plan2))) == 1  # iterable


def test_action_plan_add_returns_new_instance_and_preserves_order(make_qn):
    plan = ActionPlan(make_qn())
    plan2 = plan.add(AddColumn(Column("A", integer())))
    plan3 = plan2.add(AddColumn(Column("B", integer())))
    assert [type(a).__name__ for a in plan3] == ["AddColumn", "AddColumn"]
    assert plan is not plan2 and plan2 is not plan3  # immutability


def test_action_plan_str_empty_and_nonempty_forms(make_qn):
    empty = ActionPlan(make_qn())
    assert "empty" in str(empty).lower()
    nonempty = empty.add(AddColumn(Column("A", integer())))
    assert "add column a" in str(nonempty).lower()


def test_action_plan_add_rejects_non_action(make_qn):
    plan = ActionPlan(make_qn())
    with pytest.raises(TypeError):
        plan.add("not-an-action")  # type: ignore[arg-type


# ---------------------------
# ActionPlan: merge semantics
# ---------------------------

def test_extend_requires_same_target(make_qn):
    p1 = ActionPlan(make_qn("c1", "s", "t"))
    p2 = ActionPlan(make_qn("c2", "s", "t"))
    with pytest.raises(ValueError):
        _ = p1 + p2


def test_extend_with_same_target_concatenates_and_is_immutable(make_qn):
    p1 = ActionPlan(make_qn("c", "s", "t")).add(AddColumn(Column("a", integer())))
    p2 = ActionPlan(make_qn("c", "s", "t")).add(AddColumn(Column("b", integer())))
    p3 = p1 + p2
    assert [type(a).__name__ for a in p3] == ["AddColumn", "AddColumn"]
    assert str(p3).lower().count("add column") == 2
    assert p3 is not p1 and p3 is not p2


# ---------------------------
# ActionPlan: counting by type (pythonic)
# ---------------------------

def test_counts_by_action_type(make_qn):
    plan = ActionPlan(make_qn())
    plan = plan.add(AddColumn(Column("a", integer())))
    plan = plan.add(DropColumn("b"))
    plan = plan.add(AddColumn(Column("c", integer())))

    counts = plan.count_by_action()
    assert counts[AddColumn] == 2
    assert counts[DropColumn] == 1
    assert counts.get(CreateTable, 0) == 0
    # sanity: total actions counted equals number of actions
    assert sum(counts.values()) == len(tuple(plan))
