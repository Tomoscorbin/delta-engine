from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from delta_engine.application.results import (
    ExecutionOutcome,
    PlanPreview,
)

# ---------- Helpers ----------


def make_plan_stub():
    # ActionPlan is only a runtime hint; results.py doesn't introspect it.
    return SimpleNamespace(name="dummy-plan")


# ---------- PlanPreview ----------


def test_plan_preview_summary_text_empty():
    preview = PlanPreview(
        plan=make_plan_stub(),
        is_noop=True,
        summary_counts={},
        total_actions=0,
    )
    assert preview.summary_text == ""


def test_plan_preview_summary_text_sorted_and_formatted():
    preview = PlanPreview(
        plan=make_plan_stub(),
        is_noop=False,
        summary_counts={"create_table": 1, "add_column": 2, "drop_column": 3},
        total_actions=6,
    )
    # Keys should be sorted alphabetically
    assert preview.summary_text == "add_column=2 create_table=1 drop_column=3"


def test_plan_preview_truthiness():
    p0 = PlanPreview(plan=make_plan_stub(), is_noop=True, summary_counts={}, total_actions=0)
    p3 = PlanPreview(plan=make_plan_stub(), is_noop=False, summary_counts={}, total_actions=3)
    assert bool(p0) is False
    assert bool(p3) is True


def test_plan_preview_is_frozen():
    preview = PlanPreview(plan=make_plan_stub(), is_noop=True, summary_counts={}, total_actions=0)
    with pytest.raises(FrozenInstanceError):
        preview.total_actions = 1  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError):
        preview.plan = make_plan_stub()  # type: ignore[attr-defined]


# ---------- ExecutionOutcome ----------


@pytest.mark.parametrize("success,expected_bool", [(True, True), (False, False)])
def test_execution_outcome_truthiness(success, expected_bool):
    outcome = ExecutionOutcome(success=success)
    assert bool(outcome) is expected_bool


def test_execution_outcome_defaults():
    outcome = ExecutionOutcome(success=True)
    assert outcome.messages == ()
    assert outcome.executed_sql == ()
    assert outcome.executed_count == 0


def test_execution_outcome_coerces_sequences_to_tuples_and_is_immutable():
    msgs = ["ok", "done"]
    sqls = ["SQL 1", "SQL 2"]
    outcome = ExecutionOutcome(
        success=True,
        messages=msgs,  # list accepted, stored as tuple
        executed_count=2,
        executed_sql=sqls,  # list accepted, stored as tuple
    )
    # Stored as tuples
    assert outcome.messages == ("ok", "done")
    assert outcome.executed_sql == ("SQL 1", "SQL 2")
    assert isinstance(outcome.messages, tuple)
    assert isinstance(outcome.executed_sql, tuple)
    # Mutating caller lists does not affect stored data
    msgs.append("later")
    sqls.append("later")
    assert outcome.messages == ("ok", "done")
    assert outcome.executed_sql == ("SQL 1", "SQL 2")


def test_execution_outcome_is_frozen():
    outcome = ExecutionOutcome(success=True)
    with pytest.raises(FrozenInstanceError):
        outcome.executed_count = 5  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError):
        outcome.messages = ("x",)  # type: ignore[attr-defined]



