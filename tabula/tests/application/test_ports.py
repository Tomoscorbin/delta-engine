from __future__ import annotations

from tabula.application.ports import CatalogReader, PlanExecutor
from tabula.application.results import ExecutionOutcome
from tabula.domain.plan.actions import ActionPlan
from tabula.domain.model import QualifiedName, ObservedTable


# --- Positive conformance fakes ---


class FakeReader:
    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None:
        # return None (not found) to keep it side-effect free
        return None


class FakeExecutor:
    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        # Success outcome; executed_count mirrors len(plan)
        return ExecutionOutcome(success=True, messages=("ok",), executed_count=len(plan))


def test_fake_reader_conforms_and_returns_observed_or_none():
    r = FakeReader()
    assert isinstance(r, CatalogReader)
    assert r.fetch_state(QualifiedName("c", "s", "t")) is None  # sanity


def test_fake_executor_conforms_and_returns_execution_outcome():
    e = FakeExecutor()
    assert isinstance(e, PlanExecutor)
    plan = ActionPlan(QualifiedName("c", "s", "t"))
    out = e.execute(plan)
    assert isinstance(out, ExecutionOutcome)
    assert bool(out) is True  # truthiness reflects success policy
    assert out.messages == ("ok",)
    assert out.executed_count == len(plan)


# --- Negative conformance: missing methods are rejected ---


class NoFetchState:
    pass


class NoExecute:
    pass


def test_objects_missing_required_methods_do_not_conform():
    assert not isinstance(NoFetchState(), CatalogReader)
    assert not isinstance(NoExecute(), PlanExecutor)


# --- Document runtime_chec_
