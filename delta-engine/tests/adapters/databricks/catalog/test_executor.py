from __future__ import annotations

from unittest.mock import MagicMock, call

import delta_engine.adapters.databricks.catalog.executor as exec_mod
from delta_engine.adapters.databricks.catalog.executor import DatabricksExecutor
from delta_engine.application.results import ActionStatus, ExecutionResult
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan, AddColumn, DropColumn


def make_spark_mock() -> MagicMock:
    """Strict mock that only exposes .sql."""
    spark = MagicMock(spec=["sql"])  # only allow .sql; AttributeErrors for anything else
    spark.sql.return_value = None  # whatever your executor expects; adjust if needed
    return spark


def _plan_two() -> ActionPlan:
    qn = QualifiedName("dev", "silver", "people")
    actions = (
        AddColumn(Column("age", Integer(), is_nullable=False)),
        DropColumn("nickname"),
    )
    return ActionPlan(target=qn, actions=actions)


def test_execute_success_returns_one_result_per_action_and_ok(monkeypatch) -> None:
    def fake_compile_plan(plan: ActionPlan) -> tuple[str, str]:
        return ("ALTER ... ADD", "ALTER ... DROP")

    monkeypatch.setattr(exec_mod, "compile_plan", fake_compile_plan)

    spark = make_spark_mock()
    ex = DatabricksExecutor(spark)

    results = ex.execute(_plan_two())

    assert isinstance(results, tuple)
    assert len(results) == 2
    assert all(isinstance(r, ExecutionResult) for r in results)
    assert [r.status for r in results] == [ActionStatus.OK, ActionStatus.OK]
    assert [r.action for r in results] == ["AddColumn", "DropColumn"]
    assert results[0].statement_preview.startswith("ALTER")
    assert results[1].statement_preview.startswith("ALTER")

    spark.sql.assert_has_calls([call("ALTER ... ADD"), call("ALTER ... DROP")])
    assert spark.sql.call_count == 2


def test_execute_failure_on_second_action_captures_failure(monkeypatch) -> None:
    def fake_compile_plan(plan: ActionPlan) -> tuple[str, str]:
        return ("S1", "S2")

    monkeypatch.setattr(exec_mod, "compile_plan", fake_compile_plan)

    spark = make_spark_mock()

    def sql_side_effect(stmt: str):
        if stmt == "S2":
            raise ValueError("boom")
        return None

    spark.sql.side_effect = sql_side_effect

    ex = DatabricksExecutor(spark)
    results = ex.execute(_plan_two())

    assert [r.status for r in results] == [ActionStatus.OK, ActionStatus.FAILED]
    failure = results[1].failure
    assert failure is not None
    assert failure.action_index == 1
    assert failure.exception_type == "ValueError"

    spark.sql.assert_has_calls([call("S1"), call("S2")])
    assert spark.sql.call_count == 2


def test_execute_empty_plan_returns_empty_tuple(monkeypatch) -> None:
    def fake_compile_plan(plan: ActionPlan) -> tuple[()]:
        return ()

    monkeypatch.setattr(exec_mod, "compile_plan", fake_compile_plan)

    spark = make_spark_mock()
    ex = DatabricksExecutor(spark)

    empty_plan = ActionPlan(target=QualifiedName("dev", "silver", "empty"), actions=())
    results = ex.execute(empty_plan)

    assert results == ()
    spark.sql.assert_not_called()
