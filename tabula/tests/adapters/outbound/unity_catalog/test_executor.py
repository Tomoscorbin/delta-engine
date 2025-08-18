import pytest

from tabula.adapters.databricks.catalog.plan_executor import SqlPlanExecutor
from tabula.adapters.databricks.sql.compile import compile_plan
from tabula.domain.plan.actions import ActionPlan, AddColumn, DropColumn
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.model.qualified_name import QualifiedName


def qn() -> QualifiedName:
    return QualifiedName("c", "s", "t")


def plan_three() -> ActionPlan:
    return ActionPlan(
        qn(),
        actions=(
            AddColumn(Column("a", DataType("integer"))),
            AddColumn(Column("b", DataType("integer"))),
            DropColumn("c"),
        ),
    )


def test_executor_success_records_sql_messages_and_durations():
    executed = []

    def run_sql(sql: str) -> None:
        executed.append(sql)

    plan = plan_three()
    execr = SqlPlanExecutor(run_sql=run_sql, on_error="stop", dry_run=False)
    out = execr.execute(plan)

    assert out.success
    assert out.executed_count == 3
    assert tuple(executed) == out.executed_sql
    assert len(out.messages) == 3
    assert all(m.startswith("OK ") for m in out.messages)
    assert len(out.durations_ms) == 3
    assert all(isinstance(x, float) for x in out.durations_ms)


def test_executor_stop_on_error_stops_after_failure():
    calls = {"n": 0}

    def run_sql(_sql: str) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")

    plan = plan_three()
    execr = SqlPlanExecutor(run_sql=run_sql, on_error="stop")
    out = execr.execute(plan)

    assert not out.success
    assert out.executed_count == 1
    assert len(out.executed_sql) == 1
    assert any("ERROR 2" in m for m in out.messages)
    # Durations length matches number of attempts (2)
    assert len(out.durations_ms) == 2


def test_executor_continue_on_error_attempts_all():
    def run_sql(sql: str) -> None:
        second = compile_plan(ActionPlan(qn(), actions=(AddColumn(Column("b", DataType("integer"))),)))[0]
        if sql == second:
            raise ValueError("expected failure")

    plan = plan_three()
    execr = SqlPlanExecutor(run_sql=run_sql, on_error="continue")
    out = execr.execute(plan)

    assert not out.success
    assert out.executed_count == 2  # first add + drop succeeded
    assert len(out.executed_sql) == 2
    assert any(m.startswith("OK ") for m in out.messages)
    assert any("ERROR " in m for m in out.messages)
    assert len(out.durations_ms) == 3  # three attempts


def test_executor_dry_run_executes_nothing_and_returns_statements():
    calls = {"n": 0}

    def run_sql(_sql: str) -> None:
        calls["n"] += 1

    plan = ActionPlan(qn(), actions=(AddColumn(Column("a", DataType("integer"))),))
    execr = SqlPlanExecutor(run_sql=run_sql, dry_run=True)
    out = execr.execute(plan)

    assert out.success
    assert calls["n"] == 0
    assert out.executed_count == 0
    assert len(out.executed_sql) == 1  # one would-be statement
    assert out.messages and out.messages[0].startswith("DRY RUN 1: ")
    assert out.durations_ms == (0.0,)
