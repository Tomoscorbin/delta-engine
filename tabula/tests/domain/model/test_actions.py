import pytest
from tabula.domain.model.actions import AddColumn, DropColumn, CreateTable, ActionPlan
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.types import integer
from tabula.domain.model.column import Column
from tests.conftest import qn, col


def qn(a="Cat", b="Sch", c="Tbl") -> QualifiedName:
    return QualifiedName(a, b, c)

def test_add_drop_str_and_normalization():
    a = AddColumn(col("X"))
    d = DropColumn("X")
    assert "add column x" in str(a)
    assert str(d) == "drop column x"

def test_create_table_mentions_count():
    c = CreateTable((col("a"), col("b")))
    assert "2 column(s)" in str(c)

def test_container_niceties_and_truthiness():
    plan = ActionPlan(qn())
    assert not plan
    plan2 = plan.add(AddColumn(col("a")))
    assert plan2
    assert len(tuple(iter(plan2))) == 1

def test_extend_requires_same_target():
    p1 = ActionPlan(qn("c1","s","t"))
    p2 = ActionPlan(qn("c2","s","t"))
    with pytest.raises(ValueError):
        _ = p1 + p2


def test_drop_column_name_normalized():
    d = DropColumn("UserID")
    assert d.column_name == "userid"
    assert "userid" in str(d)

def test_create_table_message_includes_count_and_is_human_friendly():
    c = CreateTable((Column("A", integer()), Column("B", integer())))
    s = str(c)
    assert "create table" in s and "2 column" in s

def test_action_plan_iadd_returns_new_instance_and_preserves_order():
    plan = ActionPlan(qn())
    plan2 = plan.add(AddColumn(Column("A", integer())))
    plan3 = plan2.add(AddColumn(Column("B", integer())))
    assert [type(a).__name__ for a in plan3] == ["AddColumn", "AddColumn"]
    assert plan is not plan2 and plan2 is not plan3  # immutability

def test_action_plan_str_empty_and_nonempty_forms():
    empty = ActionPlan(qn())
    assert "empty" in str(empty)
    nonempty = empty.add(AddColumn(Column("A", integer())))
    # human-readable listing
    assert "add column a" in str(nonempty)