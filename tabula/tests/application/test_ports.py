from typing import Optional
from tabula.application.ports import CatalogReader, PlanExecutor
from tabula.application.results import ExecutionOutcome
from tabula.domain.model.actions import ActionPlan
from tabula.domain.model.table import ObservedTable
from tabula.domain.model.qualified_name import QualifiedName

# Fakes used only to test protocol conformance
class FakeReader:
    def fetch_state(self, qualified_name: QualifiedName) -> Optional[ObservedTable]:
        return None

class FakeExecutor:
    def execute(self, plan: ActionPlan) -> ExecutionOutcome:
        return ExecutionOutcome(success=True, messages=("ok",), executed_count=len(plan))

def test_fake_reader_conforms_to_protocol():
    r = FakeReader()
    assert isinstance(r, CatalogReader)

def test_fake_executor_conforms_to_protocol():
    e = FakeExecutor()
    assert isinstance(e, PlanExecutor)
