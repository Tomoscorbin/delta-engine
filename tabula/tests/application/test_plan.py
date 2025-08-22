from __future__ import annotations
import pytest
from types import SimpleNamespace

from tabula.application.plan import plan_actions


# --- Test doubles ---

class StubReader:
    def __init__(self, observed_state):
        self._observed_state = observed_state
        self.called_with = None

    def fetch_state(self, qualified_name: str):
        self.called_with = qualified_name
        return self._observed_state


class FakePlan:
    """Minimal plan for tests: length, truthiness via __len__, and counts."""
    def __init__(self, n_actions: int, counts: dict[str, int]):
        self._n = n_actions
        self._counts = counts

    def __len__(self):
        return self._n

    def count_by_action(self):
        return self._counts


# --- Helpers ---

def make_desired(qn: str = "cat.schema.table_x"):
    # Matches what plan_actions reads
    return SimpleNamespace(qualified_name=qn)


# --- Tests ---

def test_plan_actions_orders_plan_and_preserves_counts_and_total(monkeypatch):
    desired = make_desired("cat.schema.table_x")
    observed_state = SimpleNamespace(state="observed")
    reader = StubReader(observed_state)

    call = {}

    # Return a non-iterable FakePlan so we *must* go through order_plan
    original_plan = FakePlan(3, {"add_column": 2, "create_table": 1})

    def fake_diff(observed, desired_table):
        call["observed"] = observed
        call["desired"] = desired_table
        return original_plan

    ordered_sentinel = object()  # stand-in for the ordered plan

    def fake_order_plan(plan):
        call["ordered_with"] = plan
        return ordered_sentinel

    monkeypatch.setattr("tabula.application.plan.diff", fake_diff)
    monkeypatch.setattr("tabula.application.plan.order_plan", fake_order_plan)

    # Act
    preview = plan_actions(desired, reader)

    # Reader + diff interactions
    assert reader.called_with == "cat.schema.table_x"
    assert call["observed"] is observed_state
    assert call["desired"] is desired
    # order_plan called with the *original* plan
    assert call["ordered_with"] is original_plan

    # Preview shows ordered plan, but counts/total from original
    assert preview.plan is ordered_sentinel
    assert preview.is_noop is False
    assert preview.summary_counts is original_plan.count_by_action()
    assert preview.total_actions == len(original_plan)


def test_plan_actions_noop_when_plan_empty_and_still_runs_order(monkeypatch):
    desired = make_desired("c.s.empty")
    reader = StubReader(observed_state=None)

    empty_plan = FakePlan(0, {})
    monkeypatch.setattr("tabula.application.plan.diff", lambda o, d: empty_plan)

    # Even for empty plans, order_plan should be invoked and can return same instance
    called = {}
    def fake_order_plan(plan):
        called["plan"] = plan
        return plan
    monkeypatch.setattr("tabula.application.plan.order_plan", fake_order_plan)

    preview = plan_actions(desired, reader)

    assert called["plan"] is empty_plan
    assert preview.plan is empty_plan
    assert preview.is_noop is True
    assert preview.summary_counts == {}
    assert preview.total_actions == 0


def test_plan_actions_passes_through_counts_identity(monkeypatch):
    desired = make_desired("c.s.t")
    reader = StubReader(observed_state=None)

    class WeirdCounts(dict):
        pass

    counts = WeirdCounts({"drop_column": 1})
    plan = FakePlan(1, counts)

    monkeypatch.setattr("tabula.application.plan.diff", lambda o, d: plan)
    monkeypatch.setattr("tabula.application.plan.order_plan", lambda p: p)

    preview = plan_actions(desired, reader)

    # exact object identity, not a copy
    assert preview.summary_counts is counts
    assert preview.total_actions == 1
    assert preview.is_noop is False


def test_plan_actions_bubbles_diff_errors(monkeypatch):
    desired = make_desired("c.s.t")
    reader = StubReader(observed_state=None)

    def boom(observed, desired_table):
        raise RuntimeError("diff blew up")

    monkeypatch.setattr("tabula.application.plan.diff", boom)

    with pytest.raises(RuntimeError, match="diff blew up"):
        plan_actions(desired, reader)
