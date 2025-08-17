import pytest
from dataclasses import dataclass
from typing import Optional, List

# --- application layer ---
from tabula.application.execute import execute_plan
from tabula.application.plan import plan_actions

from tabula.application.ports import CatalogReader
from tabula.application.errors import ExecutionFailed

# --- outbound SQL adapter ---
from tabula.adapters.outbound.unity_catalog.sql.executor import SqlPlanExecutor

# --- domain model ---
from tabula.domain.model.full_name import FullName
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.table_state import TableState
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType


# ---------- helpers ----------

def make_full_name() -> FullName:
    return FullName("dev", "sales", "orders")

def col(name: str, dtype: str = "string", nullable: bool = True) -> Column:
    # dtype can be "decimal" by passing DataType("decimal", (p,s)) manually below if needed
    return Column(name=name, data_type=DataType(dtype) if "(" not in dtype else DataType("decimal", (18, 2)), is_nullable=nullable)

def spec(cols: list[Column]) -> TableSpec:
    return TableSpec(full_name=make_full_name(), columns=tuple(cols))

def state(cols: list[Column]) -> TableState:
    return TableState(full_name=make_full_name(), columns=tuple(cols))


# Fake CatalogReader that satisfies the Protocol
@dataclass
class FakeCatalog(CatalogReader):
    by_name: dict[str, TableState]
    def fetch_state(self, full_name: FullName) -> Optional[TableState]:
        # prefer the VO method to avoid str() surprises
        return self.by_name.get(full_name.qualified_name())


# Simple callable recorder for SqlPlanExecutor
class Recorder:
    def __init__(self):
        self.sql: List[str] = []
    def __call__(self, statement: str) -> None:
        self.sql.append(statement)


# ---------- e2e tests ----------

def test_e2e_create_then_execute_sql():
    # No table in catalog -> plan should create
    reader = FakeCatalog(by_name={})
    rec = Recorder()
    executor = SqlPlanExecutor(run_sql=rec)

    desired = spec([Column("id", DataType("integer"), is_nullable=False)])
    preview = plan_actions(desired, reader)

    assert preview.is_noop is False
    # summary uses snake_case action names
    assert preview.summary == "create_table=1"

    result = execute_plan(desired, reader, executor)
    assert result.success is True

    # For MVP we omit nullability in DDL
    assert rec.sql == [
        "CREATE TABLE dev.sales.orders (id INT)"
    ]


def test_e2e_adds_in_spec_order_and_drop_then_execute_sql():
    # Existing table has 'a' and 'OLD'; desired adds b,c and drops OLD
    reader = FakeCatalog(by_name={
        make_full_name().qualified_name(): state([col("a"), col("OLD")])
    })
    rec = Recorder()
    executor = SqlPlanExecutor(run_sql=rec)

    desired = spec([col("a"), col("b"), col("c")])
    preview = plan_actions(desired, reader)

    assert preview.is_noop is False
    assert preview.summary == "add_column=2 drop_column=1"

    result = execute_plan(desired, reader, executor)
    assert result.success is True

    # Adds preserve spec order; drops sorted by name (only one here)
    assert rec.sql == [
        "ALTER TABLE dev.sales.orders ADD COLUMNS (b STRING)",
        "ALTER TABLE dev.sales.orders ADD COLUMNS (c STRING)",
        "ALTER TABLE dev.sales.orders DROP COLUMN OLD",
    ]


def test_e2e_executor_failure_bubbles_as_execution_failed():
    # Force run_sql to blow up on the first statement
    class ExplodingRunner:
        def __call__(self, s: str) -> None:
            raise RuntimeError("boom")

    reader = FakeCatalog(by_name={})
    executor = SqlPlanExecutor(run_sql=ExplodingRunner())

    desired = spec([Column("id", DataType("integer"), is_nullable=False)])

    with pytest.raises(ExecutionFailed):
        execute_plan(desired, reader, executor)
