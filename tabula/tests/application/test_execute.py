import pytest
from tabula.application.execute import execute_plan, plan_then_execute
from tabula.application.errors import ExecutionFailed
from tabula.application.results import ExecutionOutcome
from tabula.application.plan import plan_actions
from tabula.application.ports import CatalogReader, PlanExecutor
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.column import Column
from tabula.domain.model.types import integer
from tabula.domain.model.actions import ActionPlan

def qn() -> QualifiedName:
    return QualifiedName("Cat", "Sch", "Tbl")

def desired(*cols: str) -> DesiredTable:
    return DesiredTable(qualified_name=qn(), columns=tuple(Column(c, integer()) for c in cols))

def observed(*cols: str) -> ObservedTable:
    return ObservedTable(qualified_name=qn(), columns=tuple(Column(c, integer()) for c in cols))

class ReaderNone(CatalogReader):
    def fetch_state(self, qualified_name: QualifiedName): return None

class ReaderWith(CatalogReader):
    def __init__(self, table: ObservedTable) -> None:
        self._t = table
    def fetch_state(self, qualified_name: QualifiedName): return self._t

class ExecOK(PlanExecutor):
    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        # success with as many actions as in plan
        return ExecutionOutcome(success=True, messages=("ok",), executed_count=len(plan))

class ExecFail(PlanExecutor):
    def __init__(self, msg: str = "boom") -> None:
        self._msg = msg
    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        # simulate failure after doing some work
        return ExecutionOutcome(success=False, messages=(self._msg,), executed_count=max(0, len(plan) - 1))

def test_execute_plan_success_returns_result_with_counts():
    prev = plan_actions(desired("a", "b"), reader=ReaderNone())
    res = execute_plan(prev, executor=ExecOK())
    assert res.executed_count == len(prev.plan)
    assert "ok" in res.messages

def test_execute_plan_failure_raises_with_context():
    prev = plan_actions(desired("a"), reader=ReaderNone())
    with pytest.raises(ExecutionFailed) as exc:
        _ = execute_plan(prev, executor=ExecFail("bad"))
    s = str(exc.value)
    assert "bad" in s
    assert str(prev.plan.qualified_name) in s

def test_plan_then_execute_returns_noop_without_calling_executor_when_no_actions(monkeypatch):
    prev = plan_actions(desired("a"), reader=ReaderWith(observed("A")))
    # guard: if noop, execute_plan shouldn't be invoked (we check indirectly by using a failing executor)
    res = plan_then_execute(desired_table=desired("a"), reader=ReaderWith(observed("A")), executor=ExecFail("should not run"))
    # It should be a noop result; message can be 'noop' per our application policy
    assert res.executed_count == 0
    assert "noop" in res.messages
