import pytest

from tabula.adapters.outbound.unity_catalog.sql.executor import SqlPlanExecutor
from tabula.adapters.outbound.unity_catalog.sql.compile import compile_plan
from tabula.domain.model.actions import ActionPlan, CreateTable, AddColumn, DropColumn
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType


def qn() -> QualifiedName:
    return QualifiedName("c", "s", "t")


def plan_three_statements() -> ActionPlan:
    return ActionPlan(
        qualified_name=qn(),
        actions=(
            AddColumn(Column("a", DataType("int"))),
            AddColumn(Column("b", DataType("int"))),
            DropColumn("c"),
        ),
    )


def test_executor_success_paths_return_executed_sql_and_messages():
    executed = []
    def run_sql(sql: str) -> None:
        executed.append(sql)

    plan = plan_three_statements()
    execr = SqlPlanExecutor(run_sql=run_sql, on_error="stop", dry_run=False)
    outcome = execr.execute(plan)

    # Outcome truthiness and counts
    assert outcome
    assert outcome.executed_count == 3
    # SQL separated from messages
    assert len(outcome.executed_sql) == 3
    for stmt in outcome.executed_sql:
        assert stmt in executed  # ran in order
    # messages contain OK entries
    assert any(m.startswith("OK 1") for m in outcome.messages)


def test_executor_stop_on_error_halts_early_and_reports_error():
    calls = {"n": 0}
    def run_sql(sql: str) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")  # fail on 2nd statement

    plan = plan_three_statements()
    execr = SqlPlanExecutor(run_sql=run_sql, on_error="stop")
    outcome = execr.execute(plan)

    assert not outcome
    # Only the first succeeded
    assert outcome.executed_count == 1
    assert len(outcome.executed_sql) == 1
    # Messages include an ERROR for statement 2
    assert any("ERROR 2" in m for m in outcome.messages)


def test_executor_continue_on_error_attempts_all_statements_and_aggregates():
    failures = {2}
    def run_sql(sql: str) -> None:
        # fail only on second statement
        if compile_plan(ActionPlan(qn(), actions=(AddColumn(Column("b", DataType("int"))),)))[0] == sql:
            raise ValueError("expected failure")

    plan = plan_three_statements()
    execr = SqlPlanExecutor(run_sql=run_sql, on_error="continue")
    outcome = execr.execute(plan)

    assert not outcome
    # Two succeeded (1st add + drop), one failed
    assert outcome.executed_count == 2
    assert len(outcome.executed_sql) == 2
    # Messages contain an error and at least one OK
    assert any(m.startswith("OK ") for m in outcome.messages)
    assert any("ERROR " in m for m in outcome.messages)
