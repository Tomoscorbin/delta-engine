import pytest

from tabula.domain.plan.actions import ActionPlan, AddColumn, DropColumn, CreateTable
from tabula.domain.services.differ import diff
from tabula.domain.model.column import Column
from tabula.domain.model.types import integer, string
from tabula.domain.model.table import DesiredTable, ObservedTable


def make_col(name: str, *, dtype=None, is_nullable: bool = True) -> Column:
    return Column(name, dtype or integer(), is_nullable=is_nullable)


def make_desired(qn, cols: tuple[Column, ...]) -> DesiredTable:
    return DesiredTable(qn, cols)


def make_observed(qn, cols: tuple[Column, ...], is_empty: bool | None = None) -> ObservedTable:
    # ObservedTable may or may not have is_empty; default to signature shown earlier.
    try:
        return ObservedTable(qn, cols, is_empty=is_empty)  # type: ignore[arg-type]
    except TypeError:
        return ObservedTable(qn, cols, is_empty=False)  # fallback if no is_empty field


def test_when_observed_is_none_plan_creates_table(make_qn):
    qn = make_qn()
    desired = make_desired(qn, (make_col("A"), make_col("B")))
    plan = diff(None, desired)

    assert isinstance(plan, ActionPlan)
    assert plan.target == qn
    assert plan  # non-empty
    assert len(tuple(plan)) == 1
    action = tuple(plan)[0]
    assert isinstance(action, CreateTable)
    assert [c.name for c in action.columns] == ["a", "b"]


def test_raises_when_qualified_name_mismatch(make_qn):
    desired = make_desired(make_qn("c1", "s", "t"), (make_col("a"),))
    observed = make_observed(make_qn("c2", "s", "t"), (make_col("a"),))
    with pytest.raises(ValueError):
        _ = diff(observed, desired)


def test_empty_plan_when_schemas_match_case_insensitively(make_qn):
    qn = make_qn()
    desired = make_desired(qn, (make_col("A"), make_col("B")))
    observed = make_observed(qn, (make_col("b"), make_col("a")))
    plan = diff(observed, desired)

    assert isinstance(plan, ActionPlan)
    assert plan.target == qn
    assert not plan  # empty
    assert tuple(plan) == ()


def test_plan_contains_adds_and_drops_from_column_diff(make_qn):
    qn = make_qn()
    desired = make_desired(qn, (make_col("a"), make_col("c"), make_col("d")))
    observed = make_observed(qn, (make_col("a"), make_col("b"), make_col("c")))
    plan = diff(observed, desired)

    actions = list(plan)
    assert [type(a) for a in actions] == [AddColumn, DropColumn]
    add = actions[0]
    drop = actions[1]
    assert add.column.name == "d"
    assert drop.column_name == "b"


def test_type_and_nullability_differences_are_ignored(make_qn):
    qn = make_qn()
    desired = make_desired(qn, (Column("id", string(), is_nullable=False),))
    observed = make_observed(qn, (Column("id", integer(), is_nullable=True),))
    plan = diff(observed, desired)
    assert not plan
    assert tuple(plan) == ()


def test_is_empty_flag_on_observed_does_not_change_diff(make_qn):
    qn = make_qn()
    desired = make_desired(qn, (make_col("a"),))
    observed_nonempty = make_observed(qn, (make_col("a"), make_col("z")), is_empty=False)
    observed_empty = make_observed(qn, (make_col("a"), make_col("z")), is_empty=True)

    plan1 = diff(observed_nonempty, desired)
    plan2 = diff(observed_empty, desired)

    names1 = [a.column_name for a in plan1 if isinstance(a, DropColumn)]
    names2 = [a.column_name for a in plan2 if isinstance(a, DropColumn)]
    assert names1 == ["z"] and names2 == ["z"]
