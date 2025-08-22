from __future__ import annotations

import pytest

from tabula.domain.services.differ import diff
from tabula.domain.plan.actions import ActionPlan, CreateTable, AddColumn, DropColumn
from tabula.domain.model.column import Column
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.model.types import integer, string



def col(name: str, *, dt=integer(), is_nullable: bool = True) -> Column:
    return Column(name, dt, is_nullable=is_nullable)


# ---------------------------
# observed is None -> CREATE TABLE with all desired columns
# ---------------------------

def test_when_observed_is_none_we_emit_single_create_with_original_columns(make_qn):
    desired = DesiredTable(make_qn(), (col("id"), col("name")))
    plan = diff(None, desired)

    assert isinstance(plan, ActionPlan)
    assert len(plan) == 1
    action = plan.actions[0]
    assert isinstance(action, CreateTable)

    # columns carried through exactly (identity & order)
    assert action.columns is desired.columns
    assert [c.name for c in action.columns] == ["id", "name"]

    # target is the desired qualified name
    assert plan.target == desired.qualified_name


# ---------------------------
# qualified name mismatch -> hard error
# ---------------------------

def test_mismatched_qualified_names_raise(make_qn):
    desired = DesiredTable(make_qn("c1", "s", "t"), (col("id"),))
    observed = ObservedTable(make_qn("c2", "s", "t"), (col("id"),), is_empty=False)

    with pytest.raises(ValueError):
        _ = diff(observed, desired)


# ---------------------------
# only adds / only drops / both
# ---------------------------

def test_only_adds_are_emitted(make_qn):
    desired = DesiredTable(make_qn(), (col("a"), col("b")))
    observed = ObservedTable(make_qn(), (col("a"),), is_empty=False)

    plan = diff(observed, desired)
    kinds = tuple(type(a) for a in plan.actions)
    names = tuple(getattr(a, "column", None).name if isinstance(a, AddColumn) else None for a in plan.actions)

    assert kinds == (AddColumn,)
    assert names == ("b",)


def test_only_drops_are_emitted(make_qn):
    desired = DesiredTable(make_qn(), (col("a"),))
    observed = ObservedTable(make_qn(), (col("a"), col("b")), is_empty=False)

    plan = diff(observed, desired)

    # Don't assume ordering; assert via sets
    assert {type(a).__name__ for a in plan.actions} == {"DropColumn"}
    assert {a.column_name for a in plan.actions if isinstance(a, DropColumn)} == {"b"}


def test_adds_and_drops_both_emitted(make_qn):
    desired = DesiredTable(make_qn(), (col("b"), col("c")))
    observed = ObservedTable(make_qn(), (col("a"), col("b")), is_empty=False)

    plan = diff(observed, desired)

    # Compare by payload, not order
    kinds = {type(a).__name__ for a in plan.actions}
    payloads = {
        (a.column.name if isinstance(a, AddColumn) else a.column_name)
        for a in plan.actions
    }
    assert kinds == {"AddColumn", "DropColumn"}
    assert payloads == {"c", "a"}


# ---------------------------
# equal sets (ignoring order) -> empty plan, falsy
# ---------------------------

def test_equal_column_sets_yield_empty_plan(make_qn):
    desired = DesiredTable(make_qn(), (col("a"), col("b")))
    observed = ObservedTable(make_qn(), (col("b"), col("a")), is_empty=True)  # is_empty intentionally ignored

    plan = diff(observed, desired)
    assert len(plan) == 0
    assert not plan


# ---------------------------
# case-insensitivity on names
# ---------------------------

def test_case_insensitive_names_do_not_trigger_changes(make_qn):
    desired = DesiredTable(make_qn(), (col("ID"),))
    observed = ObservedTable(make_qn(), (col("id"),), is_empty=False)

    plan = diff(observed, desired)
    assert plan.actions == ()
