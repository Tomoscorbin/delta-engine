import pytest

from delta_engine.adapters.databricks.catalog import executor as executor_module
from delta_engine.adapters.databricks.catalog.executor import DatabricksExecutor
from delta_engine.application.results import ActionStatus
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    SetProperty,
)
from tests.factories import make_qualified_name

# --- minimal Spark stub -------------------------------------------------------


class StubSpark:
    def __init__(self, behavior: dict[str, BaseException] | None = None) -> None:
        self.behavior = behavior or {}
        self.calls: list[str] = []

    def sql(self, statement: str):
        self.calls.append(statement)
        exc = self.behavior.get(statement)
        if exc:
            raise exc


# --- helpers ------------------------------------------------------------------


def make_plan(actions: tuple) -> ActionPlan:
    qn = make_qualified_name("dev", "silver", "people")
    return ActionPlan(qn, actions)


# --- tests --------------------------------------------------------------------


def test_execute_empty_plan_returns_empty_tuple() -> None:
    spark = StubSpark()
    executor = DatabricksExecutor(spark)
    empty_plan = make_plan(tuple())

    assert executor.execute(empty_plan) == ()


def test_execute_runs_each_statement_and_returns_ok_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Plan with two actions
    plan = make_plan(
        (
            AddColumn(Column("age", Integer())),
            SetProperty("owner", "asda"),
        )
    )

    # Stub compilation and preview helpers
    monkeypatch.setattr(executor_module, "compile_plan", lambda _plan: ("S1", "S2"))
    monkeypatch.setattr(executor_module, "sql_preview", lambda s: f"PREVIEW[{s}]")

    spark = StubSpark()
    executor = DatabricksExecutor(spark)

    results = executor.execute(plan)

    assert len(results) == 2
    # Spark called with both statements in order returned by compile_plan
    assert spark.calls == ["S1", "S2"]

    # Result 0 corresponds to first action
    r0, r1 = results
    assert r0.action == "AddColumn"
    assert r0.action_index == 0
    assert r0.status is ActionStatus.OK
    assert r0.statement_preview == "PREVIEW[S1]"
    assert r0.failure is None

    # Result 1 corresponds to second action
    assert r1.action == "SetProperty"
    assert r1.action_index == 1
    assert r1.status is ActionStatus.OK
    assert r1.statement_preview == "PREVIEW[S2]"
    assert r1.failure is None


def test_execute_failure_produces_failed_result_and_uses_error_preview(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = make_plan((SetProperty("owner", "asda"),))

    monkeypatch.setattr(executor_module, "compile_plan", lambda _plan: ("FAIL",))
    monkeypatch.setattr(executor_module, "sql_preview", lambda s: f"PREV[{s}]")
    monkeypatch.setattr(executor_module, "error_preview", lambda exc: f"ERR[{type(exc).__name__}]")

    spark = StubSpark(behavior={"FAIL": ValueError("boom")})
    executor = DatabricksExecutor(spark)

    (result,) = executor.execute(plan)

    assert result.action == "SetProperty"
    assert result.action_index == 0
    assert result.status is ActionStatus.FAILED
    assert result.statement_preview == "PREV[FAIL]"
    assert result.failure is not None
    assert result.failure.action_index == 0
    assert result.failure.exception_type == "ValueError"
    assert result.failure.message == "ERR[ValueError]"


def test_execute_raises_when_compiled_statement_count_mismatches_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Two actions but only one compiled statement → zip(strict=True) should raise
    plan = make_plan(
        (
            AddColumn(Column("age", Integer())),
            SetProperty("owner", "asda"),
        )
    )
    monkeypatch.setattr(executor_module, "compile_plan", lambda _plan: ("ONLY_ONE",))

    executor = DatabricksExecutor(StubSpark())

    with pytest.raises(ValueError):
        _ = executor.execute(plan)
