import pytest
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

from tabula.application.results import (
    PlanPreview,
    ExecutionOutcome,
    ExecutionResult,
)


# ---------- Helpers ----------

def make_plan_stub():
    # ActionPlan is only a runtime hint; results.py doesn't introspect it.
    # A simple stub is enough.
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


def test_plan_preview_is_frozen():
    preview = PlanPreview(
        plan=make_plan_stub(),
        is_noop=True,
        summary_counts={},
        total_actions=0,
    )
    with pytest.raises(FrozenInstanceError):
        preview.total_actions = 1  # type: ignore[attr-defined]



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


def test_execution_outcome_no_coercion_lists_are_kept():
    outcome = ExecutionOutcome(
        success=True,
        messages=["ok", "done"],               # type: ignore[list-item]
        executed_count=2,
        executed_sql=["SQL 1", "SQL 2"],       # type: ignore[list-item]
    )
    # The implementation intentionally does not coerce; types remain as passed.
    assert isinstance(outcome.messages, list)
    assert isinstance(outcome.executed_sql, list)
    assert outcome.executed_count == 2


def test_execution_outcome_is_frozen():
    outcome = ExecutionOutcome(success=True)
    with pytest.raises(FrozenInstanceError):
        outcome.executed_count = 5  # type: ignore[attr-defined]


# ---------- ExecutionResult ----------

def test_execution_result_defaults():
    result = ExecutionResult(plan=make_plan_stub())
    assert result.messages == ()
    assert result.executed_count == 0


def test_execution_result_no_coercion():
    result = ExecutionResult(
        plan=make_plan_stub(),
        messages=["one", "two"],  # type: ignore[list-item]
        executed_count=7,
    )
    assert isinstance(result.messages, list)
    assert result.executed_count == 7


def test_execution_result_is_frozen():
    result = ExecutionResult(plan=make_plan_stub())
    with pytest.raises(FrozenInstanceError):
        result.executed_count = 99  # type: ignore[attr-defined]