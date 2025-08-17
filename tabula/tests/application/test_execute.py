import pytest
from dataclasses import dataclass
from typing import Optional

from tabula.application.results import ExecutionOutcome, ExecutionResult
from tabula.application.execute import execute_plan
from tabula.application.errors import ExecutionFailed
from tabula.domain.model.full_name import FullName
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.model.table_state import TableState
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.actions import CreateTable


# --- helpers ---

def col(name: str, dtype: str = "string", nullable: bool = True) -> Column:
    return Column(name=name, data_type=DataType(dtype), is_nullable=nullable)

def spec(cols: list[Column]) -> TableSpec:
    return TableSpec(full_name=FullName("dev", "sales", "orders"), columns=tuple(cols))

def state(cols: list[Column]) -> TableState:
    return TableState(full_name=FullName("dev", "sales", "orders"), columns=tuple(cols))


# --- fakes implementing the Protocols ---

@dataclass
class FakeCatalog:
    by_name: dict[str, TableState]
    def fetch_state(self, full_name: FullName) -> Optional[TableState]:
        return self.by_name.get(str(full_name))

@dataclass
class FakeExecutor:
    success: bool = True
    messages: tuple[str, ...] = ()
    last_plan = None
    def execute(self, plan):
        self.last_plan = plan
        return ExecutionOutcome(success=self.success, messages=self.messages, executed_count=len(plan))

def test_execute_plan_success_propagates_messages_and_plan():
    reader = FakeCatalog(by_name={})
    executor = FakeExecutor(success=True, messages=("ok",))
    s = spec([col("id", "integer", False)])

    result: ExecutionResult = execute_plan(s, reader, executor)

    assert result.success is True
    assert result.messages == ("ok",)
    assert executor.last_plan is not None
    assert isinstance(executor.last_plan.actions[0], CreateTable)

def test_execute_plan_failure_raises():
    reader = FakeCatalog(by_name={})
    executor = FakeExecutor(success=False, messages=("boom",))
    s = spec([col("id", "integer", False)])

    with pytest.raises(ExecutionFailed):
        execute_plan(s, reader, executor)
