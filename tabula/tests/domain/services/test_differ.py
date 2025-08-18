import pytest

from tabula.domain.plan.actions import ActionPlan, CreateTable, DropColumn
from tabula.domain.model.column import Column
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.model.types import integer
from tabula.domain.services.differ import diff
from tests.conftest import col, desired, observed, qn


def qn(cat: str = "Cat", sch: str = "Sch", name: str = "Tbl") -> QualifiedName:
    return QualifiedName(cat, sch, name)


def test_create_table_when_observed_missing():
    d = desired(qn(), col("a"), col("b"))
    plan = diff(None, d)
    assert isinstance(plan, ActionPlan)
    assert any(isinstance(a, CreateTable) for a in plan)


def test_mismatch_name_raises():
    d = desired(qn("c", "s", "t1"), col("a"))
    o = observed(qn("c", "s", "t2"), col("a"))
    with pytest.raises(ValueError):
        diff(o, d)


def test_add_and_drop_actions_emitted():
    d = desired(qn(), col("a"), col("b"))
    o = observed(qn(), col("a"), col("c"))
    plan = diff(o, d)
    kinds = [type(a).__name__ for a in plan]
    assert "AddColumn" in kinds
    assert "DropColumn" in kinds


def test_noop_when_schemas_match():
    d = desired(qn(), col("a"))
    o = observed(qn(), col("A"))  # case-insensitive
    plan = diff(o, d)
    assert len(list(plan)) == 0


def test_diff_creates_table_with_exact_columns_in_order_when_missing():
    desired = DesiredTable(
        qn(), (Column("A", integer()), Column("B", integer()), Column("C", integer()))
    )
    plan = diff(None, desired)
    assert any(isinstance(a, CreateTable) for a in plan)
    create = next(a for a in plan if isinstance(a, CreateTable))
    assert tuple(c.name for c in create.columns) == ("a", "b", "c")


def test_diff_raises_on_mismatched_targets():
    desired = DesiredTable(QualifiedName("c", "s", "t1"), (Column("A", integer()),))
    observed = ObservedTable(QualifiedName("c", "s", "t2"), (Column("A", integer()),))
    with pytest.raises(ValueError):
        diff(observed, desired)


def test_diff_is_idempotent_when_schemas_match():
    desired = DesiredTable(qn(), (Column("A", integer()),))
    observed = ObservedTable(qn(), (Column("a", integer()),))
    plan = diff(observed, desired)
    assert list(plan) == []


def test_diff_emits_adds_then_drops_in_that_order():
    desired = DesiredTable(qn(), (Column("A", integer()), Column("B", integer())))
    observed = ObservedTable(qn(), (Column("B", integer()), Column("C", integer())))
    plan = list(diff(observed, desired))
    assert [type(a).__name__ for a in plan[:1]] == ["AddColumn"]
    assert "DropColumn" in [type(a).__name__ for a in plan]
    # more precise: all adds appear before any drops
    first_drop_index = next(i for i, a in enumerate(plan) if isinstance(a, DropColumn))
    assert all(not isinstance(a, DropColumn) for a in plan[:first_drop_index])
