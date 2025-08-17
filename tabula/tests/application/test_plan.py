import pytest
from dataclasses import dataclass
from typing import Optional

from tabula.application.results import PlanPreview, ExecutionOutcome
from tabula.application.execute import plan_actions, execute_plan

from tabula.domain.model.full_name import FullName
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.table_state import TableState
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.model.actions import CreateTable, AddColumn, DropColumn

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
        return ExecutionOutcome(success=self.success, messages=self.messages, excuted_count=len(plan))

# --- tests ---

def test_plan_actions_creates_when_absent():
    reader = FakeCatalog(by_name={})
    s = spec([col("id", "integer", False)])
    result: PlanPreview = plan_actions(s, reader)

    assert result.is_noop is False
    assert isinstance(result.plan.actions[0], CreateTable)
    # summary uses snake_case action names
    assert result.summary == "create_table=1"

def test_plan_actions_is_noop_when_in_sync():
    reader = FakeCatalog(by_name={"dev.sales.orders": state([col("id", "integer", False)])})
    s = spec([col("id", "integer", False)])
    result = plan_actions(s, reader)

    assert result.is_noop is True
    assert len(result.plan) == 0
    assert result.summary == "noop"

def test_plan_actions_adds_in_spec_order_and_drops_alpha():
    reader = FakeCatalog(by_name={"dev.sales.orders": state([col("a"), col("OLD")])})
    s = spec([col("a"), col("b"), col("c")])

    result = plan_actions(s, reader)
    kinds = [type(a).__name__ for a in result.plan.actions]
    assert kinds == ["AddColumn", "AddColumn", "DropColumn"]

    adds = [a.column.name for a in result.plan.actions if isinstance(a, AddColumn)]
    drops = [a.column_name for a in result.plan.actions if isinstance(a, DropColumn)]
    assert adds == ["b", "c"]      # spec order
    assert drops == ["OLD"]        # alpha by name, case-insensitive
    # summary is alphabetical by action kind
    assert result.summary == "add_column=2 drop_column=1"