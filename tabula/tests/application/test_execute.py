import pytest
from types import SimpleNamespace

from tabula.application.execute import execute_plan, plan_then_execute
from tabula.application.results import PlanPreview, ExecutionResult
from tabula.application.errors import ExecutionFailed


# ---------- helpers ----------

class StubExecutor:
    def __init__(self, outcome):
        self._outcome = outcome
        self.called_with = None

    def execute(self, plan):
        self.called_with = plan
        return self._outcome


class Outcome:
    """Minimal executor outcome using truthiness."""
    def __init__(self, success: bool, messages=(), executed_count=0):
        self._success = success
        self.messages = messages
        self.executed_count = executed_count

    def __bool__(self):
        return self._success


def make_plan(target="cat.schema.tbl"):
    # App code expects a VO with `.dotted`. Mirror the domain shape here.
    class Target:
        def __init__(self, dotted: str) -> None:
            self.dotted = dotted
    return SimpleNamespace(target=Target(target))


def make_preview(plan, *, total_actions: int, is_noop: bool | None = None, counts=None):
    if counts is None:
        counts = {}
    if is_noop is None:
        is_noop = (total_actions == 0)
    return PlanPreview(
        plan=plan,
        is_noop=is_noop,
        summary_counts=counts,
        total_actions=total_actions,
    )


# ---------- PlanPreview truthiness sanity ----------

def test_plan_preview_truthiness_follows_total_actions():
    p0 = make_preview(make_plan(), total_actions=0)
    assert bool(p0) is False
    p3 = make_preview(make_plan(), total_actions=3)
    assert bool(p3) is True
    assert len(p3) == 3


# ---------- execute_plan ----------

def test_execute_plan_calls_executor_and_returns_success_result():
    plan = make_plan("c.s.success")
    preview = make_preview(plan, total_actions=3)

    outcome = Outcome(success=True, messages=("ok", "done"), executed_count=3)
    executor = StubExecutor(outcome)

    result = execute_plan(preview, executor)

    # executor called once with exact plan
    assert executor.called_with is plan

    # result mirrors outcome + keeps same plan
    assert isinstance(result, ExecutionResult)
    assert result.plan is plan
    assert result.messages == ("ok", "done")
    assert result.executed_count == 3


def test_execute_plan_raises_on_failure_with_proper_fields():
    plan = make_plan("c.s.fail_tbl")
    preview = make_preview(plan, total_actions=2)

    outcome = Outcome(success=False, messages=("bad",), executed_count=1)
    executor = StubExecutor(outcome)

    with pytest.raises(ExecutionFailed) as err:
        execute_plan(preview, executor)

    exc = err.value
    # execute_plan now uses preview.plan.target directly
    assert exc.qualified_name == "c.s.fail_tbl"
    assert exc.messages == ("bad",)
    assert exc.executed_count == 1


# ---------- plan_then_execute ----------

def test_plan_then_execute_noop_uses_falsiness_and_skips_executor(monkeypatch):
    plan = make_plan("c.s.noop")
    preview = make_preview(plan, total_actions=0)  # falsy

    captured = {}

    def fake_plan_actions(desired_table, reader):
        captured["desired"] = desired_table
        captured["reader"] = reader
        return preview

    monkeypatch.setattr("tabula.application.execute.plan_actions", fake_plan_actions)

    desired = object()
    reader = object()
    executor = StubExecutor(outcome=None)  # must not be called for no-op

    result = plan_then_execute(desired, reader, executor)

    # plan_actions was called with our args
    assert captured["desired"] is desired
    assert captured["reader"] is reader

    # executor not called
    assert executor.called_with is None

    # explicit no-op result
    assert isinstance(result, ExecutionResult)
    assert result.plan is plan
    assert result.messages == ("noop",)
    assert result.executed_count == 0


def test_plan_then_execute_delegates_to_execute_plan(monkeypatch):
    plan = make_plan("c.s.exec")
    preview = make_preview(plan, total_actions=2)  # truthy

    def fake_plan_actions(desired_table, reader):
        return preview

    sentinel = ExecutionResult(plan=plan, messages=("ran",), executed_count=2)

    captured = {}

    def fake_execute_plan(pre, ex):
        captured["preview"] = pre
        captured["executor"] = ex
        return sentinel

    monkeypatch.setattr("tabula.application.execute.plan_actions", fake_plan_actions)
    monkeypatch.setattr("tabula.application.execute.execute_plan", fake_execute_plan)

    desired = object()
    reader = object()
    executor = object()

    result = plan_then_execute(desired, reader, executor)

    assert result is sentinel
    assert captured["preview"] is preview
    assert captured["executor"] is executor
