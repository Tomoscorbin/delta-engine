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
    def __init__(self, n_actions: int, counts: dict[str, int]):
        self._n = n_actions
        self._counts = counts

    def __len__(self):
        return self._n

    # bool() will use __len__ by default

    def count_by_action(self):
        return self._counts


# --- Tests ---

def test_plan_actions_happy_path_non_noop(monkeypatch):
    # Arrange
    desired = SimpleNamespace(qualified_name="cat.schema.table_x")
    observed_state = SimpleNamespace(state="observed")
    reader = StubReader(observed_state)

    # Capture diff args and control the returned plan
    call = {}
    plan_holder = {}

    def fake_diff(observed, desired_table):
        call["observed"] = observed
        call["desired"] = desired_table
        plan = FakePlan(3, {"add_column": 2, "create_table": 1})
        plan_holder["plan"] = plan
        return plan

    monkeypatch.setattr("tabula.application.plan.diff", fake_diff)

    # Act
    preview = plan_actions(desired, reader)

    # Assert: reader + diff interactions
    assert reader.called_with == "cat.schema.table_x"
    assert call["observed"] is observed_state
    assert call["desired"] is desired

    # Assert: preview content
    assert preview.plan is plan_holder["plan"]
    assert preview.is_noop is False
    assert preview.summary_counts == {"add_column": 2, "create_table": 1}
    assert preview.total_actions == 3


def test_plan_actions_noop_when_plan_empty(monkeypatch):
    # Arrange
    desired = SimpleNamespace(qualified_name="c.s.empty_table")
    reader = StubReader(observed_state=None)

    plan_holder = {}

    def fake_diff(observed, desired_table):
        plan = FakePlan(0, {})
        plan_holder["plan"] = plan
        return plan

    monkeypatch.setattr("tabula.application.plan.diff", fake_diff)

    # Act
    preview = plan_actions(desired, reader)

    # Assert
    assert preview.plan is plan_holder["plan"]
    assert preview.is_noop is True
    assert preview.summary_counts == {}
    assert preview.total_actions == 0


def test_plan_actions_propagates_counts_directly(monkeypatch):
    """
    Sanity: whatever plan.count_by_action() returns is passed through untouched.
    Useful if callers depend on Mapping semantics (e.g., dict vs Counter).
    """
    desired = SimpleNamespace(qualified_name="c.s.t")
    reader = StubReader(observed_state=None)

    class WeirdCounts(dict):
        # A dict subclass to ensure we don't copy/convert it
        pass

    counts = WeirdCounts({"drop_column": 1})
    plan = FakePlan(1, counts)

    def fake_diff(observed, desired_table):
        return plan

    monkeypatch.setattr("tabula.application.plan.diff", fake_diff)

    # Act
    preview = plan_actions(desired, reader)

    # Assert pass-through semantics
    assert preview.summary_counts is counts
    assert preview.total_actions == 1
    assert preview.is_noop is False
