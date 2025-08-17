import pytest
from typing import Optional

from tabula.application.ports import CatalogReader, PlanExecutor
from tabula.application.results import ExecutionOutcome
from tabula.domain.model.qualified_name import FullName
from tabula.domain.model.table_state import TableState
from tabula.domain.model.actions import ActionPlan

# --- Fakes that match the Protocols ---

class FakeCatalog:
    def __init__(self, state_by_name: dict[str, TableState]):
        self._state = state_by_name
    def fetch_state(self, full_name: FullName) -> Optional[TableState]:
        # rely on FullName.__str__ or use .qualified_name(), whichever your VO exposes
        return self._state.get(str(full_name))

class FakeExecutor:
    def __init__(self, succeed: bool = True):
        self.succeed = succeed
        self.last_plan: Optional[ActionPlan] = None
    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        self.last_plan = plan
        return ExecutionOutcome(success=self.succeed, messages=("ran",))

# --- Negatives ---

class BadCatalogNoMethod:
    # no fetch_state -> should not satisfy the protocol
    pass

class BadExecutorWrongMethod:
    def run(self, plan: ActionPlan) -> ExecutionOutcome:  # wrong name
        return ExecutionOutcome(success=True)

def test_catalog_reader_runtime_checkable_isinstance_true():
    fake = FakeCatalog(state_by_name={})
    assert isinstance(fake, CatalogReader)  # @runtime_checkable allows this

def test_plan_executor_runtime_checkable_isinstance_true():
    fake = FakeExecutor()
    assert isinstance(fake, PlanExecutor)

def test_bad_catalog_is_not_instance_of_protocol():
    bad = BadCatalogNoMethod()
    assert not isinstance(bad, CatalogReader)

def test_bad_executor_is_not_instance_of_protocol():
    bad = BadExecutorWrongMethod()
    assert not isinstance(bad, PlanExecutor)
