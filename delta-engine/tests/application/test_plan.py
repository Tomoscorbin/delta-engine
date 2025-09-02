import pytest

from delta_engine.application import plan as planning_module
from delta_engine.application.plan import (
    PlanContext,
    _compute_plan,
    make_plan_context,
)
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.model.table import DesiredTable, ObservedTable
from delta_engine.domain.plan import ActionPlan
from delta_engine.domain.plan.actions import AddColumn, DropColumn, SetProperty
from tests.factories import make_qualified_name


def _mk_tables():
    qn = make_qualified_name("dev", "silver", "people")
    desired = DesiredTable(qn, (Column("id", Integer()),))
    observed = ObservedTable(qn, (Column("id", Integer()),))
    return qn, desired, observed


def test_compute_plan_sorts_actions_via_module_sort_key(monkeypatch: pytest.MonkeyPatch) -> None:
    qn, desired, observed = _mk_tables()

    # Intentionally unsorted sequence (relative to whatever key the module uses)
    unsorted_actions = (
        DropColumn("d"),
        SetProperty("owner", "asda"),
        AddColumn(Column("age", Integer())),
    )
    expected_sorted = tuple(sorted(unsorted_actions, key=planning_module.action_sort_key))

    # Return an unsorted plan from the differ
    monkeypatch.setattr(
        planning_module,
        "diff_tables",
        lambda *, desired, observed: ActionPlan(qn, unsorted_actions),
    )

    plan = _compute_plan(desired, observed)

    assert isinstance(plan, ActionPlan)
    # Target carried through
    assert plan.target == qn
    # Actions are sorted according to the module's sort key (no hard-coded order)
    assert tuple(plan) == expected_sorted


def test_compute_plan_does_not_mutate_unsorted_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    qn, desired, observed = _mk_tables()

    original_actions = (
        AddColumn(Column("b", Integer())),
        SetProperty("a", "1"),
    )
    unsorted_plan = ActionPlan(qn, original_actions)

    monkeypatch.setattr(planning_module, "diff_tables", lambda *, desired, observed: unsorted_plan)

    _ = _compute_plan(desired, observed)

    # The source plan from diff_tables must remain unchanged (no in-place sort)
    assert unsorted_plan.actions is original_actions
    assert unsorted_plan.actions == original_actions


def test_compute_plan_handles_empty_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    qn, desired, observed = _mk_tables()

    monkeypatch.setattr(
        planning_module, "diff_tables", lambda *, desired, observed: ActionPlan(qn, ())
    )

    plan = _compute_plan(desired, observed)

    assert isinstance(plan, ActionPlan)
    assert len(plan) == 0
    assert not plan
    assert list(plan) == []


def test_make_plan_context_wires_desired_observed_and_uses_compute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    qn, desired, observed = _mk_tables()
    sentinel_plan = ActionPlan(qn, (AddColumn(Column("age", Integer())),))

    monkeypatch.setattr(planning_module, "_compute_plan", lambda d, o: sentinel_plan)

    ctx = make_plan_context(desired, observed)

    assert isinstance(ctx, PlanContext)
    # Objects are passed through unchanged
    assert ctx.desired is desired
    assert ctx.observed is observed
    # Plan is exactly what _compute_plan returned
    assert ctx.plan is sentinel_plan
