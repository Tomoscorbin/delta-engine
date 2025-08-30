from delta_engine.application import plan as plan_mod
from delta_engine.application.plan import _compute_plan, make_plan_context
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.plan.actions import ActionPlan, AddColumn, CreateTable, DropColumn
from tests.factories import make_desired_table, make_observed_table, make_qualified_name

_QN = make_qualified_name("dev", "silver", "people")
_DESIRED = make_desired_table(_QN, (Column("id", Integer()), Column("name", String())))
_OBSERVED = make_observed_table(_QN, (Column("id", Integer()), Column("name", String())))


def test_make_plan_context_delegates_to_compute_and_returns_sorted_plan(monkeypatch) -> None:
    desired = _DESIRED
    observed = _OBSERVED

    # intentionally out-of-order actions from diff
    unsorted_actions = (
        DropColumn("zzz"),
        AddColumn(Column("age", Integer())),
        CreateTable(columns=desired.columns),
    )

    def fake_diff_tables(*, desired, observed):
        # preserve target to ensure replace keeps it
        return ActionPlan(target=desired.qualified_name, actions=unsorted_actions)

    # Sort order: CreateTable (0) < AddColumn (1) < DropColumn (2)
    def fake_action_sort_key(a):
        if isinstance(a, CreateTable):
            return 0
        if isinstance(a, AddColumn):
            return 1
        if isinstance(a, DropColumn):
            return 2
        return 99

    monkeypatch.setattr(plan_mod, "diff_tables", fake_diff_tables)
    monkeypatch.setattr(plan_mod, "action_sort_key", fake_action_sort_key)

    ctx = make_plan_context(desired, observed)

    # PlanContext wiring
    assert ctx.desired is desired
    assert ctx.observed is observed

    # Sorted order is enforced
    assert tuple(type(a).__name__ for a in ctx.plan.actions) == (
        "CreateTable",
        "AddColumn",
        "DropColumn",
    )
    # Target preserved
    assert ctx.plan.target == desired.qualified_name


def test__compute_plan_sorts_and_preserves_target(monkeypatch) -> None:
    desired = _DESIRED
    observed = None  # missing table

    # Two adds around a drop to test basic ordering
    unsorted_actions = (
        AddColumn(Column("b", String())),
        DropColumn("old"),
        AddColumn(Column("a", Integer())),
    )

    def fake_diff_tables(*, desired, observed):
        return ActionPlan(target=desired.qualified_name, actions=unsorted_actions)

    # Sort: Add (1), Drop (2) — both adds tie on key
    def fake_action_sort_key(a):
        if isinstance(a, AddColumn):
            return 1
        if isinstance(a, DropColumn):
            return 2
        return 99

    monkeypatch.setattr(plan_mod, "diff_tables", fake_diff_tables)
    monkeypatch.setattr(plan_mod, "action_sort_key", fake_action_sort_key)

    result = _compute_plan(observed, desired)

    # Adds come before drop
    kinds = tuple(type(a).__name__ for a in result.actions)
    assert kinds == ("AddColumn", "AddColumn", "DropColumn")

    # Stable sort: equal-key items keep their original relative order ("b" then "a")
    add_cols = [a.column.name for a in result.actions if isinstance(a, AddColumn)]
    assert add_cols == ["b", "a"]

    # Same target preserved via dataclasses.replace
    assert result.target == desired.qualified_name


def test__compute_plan_no_actions_returns_empty_tuple(monkeypatch) -> None:
    desired = _DESIRED
    observed = _OBSERVED

    def fake_diff_tables(*, desired, observed):
        return ActionPlan(target=desired.qualified_name, actions=())

    monkeypatch.setattr(plan_mod, "diff_tables", fake_diff_tables)

    result = _compute_plan(observed, desired)
    assert result.actions == ()
    assert bool(result) is False  # ActionPlan.__bool__ delegates to actions


def test_iteration_and_len_of_sorted_plan(monkeypatch) -> None:
    desired = _DESIRED

    unsorted_actions = (
        DropColumn("x"),
        AddColumn(Column("y", Integer())),
    )

    def fake_diff_tables(*, desired, observed):
        return ActionPlan(target=desired.qualified_name, actions=unsorted_actions)

    # Sort puts Add before Drop
    def fake_action_sort_key(a):
        return 0 if isinstance(a, AddColumn) else 1

    monkeypatch.setattr(plan_mod, "diff_tables", fake_diff_tables)

    plan = _compute_plan(None, desired)
    assert len(plan) == 2
    assert [type(a).__name__ for a in list(plan)] == ["AddColumn", "DropColumn"]
