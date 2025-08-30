import delta_engine.adapters.databricks.catalog.executor as exec_mod
from delta_engine.adapters.databricks.catalog.executor import DatabricksExecutor
from delta_engine.application.results import ActionStatus, ExecutionResult
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan, AddColumn, DropColumn

# --- stubs ----------------------------------------------------------------


class _StubSpark:
    def __init__(self):
        self.statements: list[str] = []
        self.to_fail_at_index: set[int] = set()

    def sql(self, stmt: str):
        idx = len(self.statements)
        self.statements.append(stmt)
        if idx in self.to_fail_at_index:
            raise ValueError(f"bad sql at {idx}")


def _plan_two() -> ActionPlan:
    qn = QualifiedName("dev", "silver", "people")
    actions = (
        AddColumn(Column("age", Integer(), is_nullable=False)),
        DropColumn("nickname"),
    )
    return ActionPlan(target=qn, actions=actions)


# --- tests ---------------------------------------------------------------------


def test_execute_success_returns_one_result_per_action_and_ok(monkeypatch) -> None:
    # Arrange: compile_plan returns one statement per action
    def fake_compile_plan(plan: ActionPlan):
        return ("ALTER ... ADD", "ALTER ... DROP")

    monkeypatch.setattr(exec_mod, "compile_plan", fake_compile_plan)

    spark = _StubSpark()
    ex = DatabricksExecutor(spark)

    results = ex.execute(_plan_two())

    assert isinstance(results, tuple)
    assert len(results) == 2
    assert all(isinstance(r, ExecutionResult) for r in results)
    assert [r.status for r in results] == [ActionStatus.OK, ActionStatus.OK]
    assert [r.action for r in results] == ["AddColumn", "DropColumn"]
    assert results[0].statement_preview.startswith("ALTER")
    assert results[1].statement_preview.startswith("ALTER")


def test_execute_failure_on_second_action_captures_failure(monkeypatch) -> None:
    def fake_compile_plan(plan: ActionPlan):
        return ("S1", "S2")

    monkeypatch.setattr(exec_mod, "compile_plan", fake_compile_plan)

    spark = _StubSpark()
    spark.to_fail_at_index.add(1)  # second statement fails
    ex = DatabricksExecutor(spark)

    results = ex.execute(_plan_two())

    assert [r.status for r in results] == [ActionStatus.OK, ActionStatus.FAILED]
    fail = results[1].failure
    assert fail is not None
    assert fail.action_index == 1
    assert fail.exception_type in {"ValueError"}


def test_execute_empty_plan_returns_empty_tuple(monkeypatch) -> None:
    def fake_compile_plan(plan: ActionPlan):
        return ()

    monkeypatch.setattr(exec_mod, "compile_plan", fake_compile_plan)

    spark = _StubSpark()
    ex = DatabricksExecutor(spark)

    empty_plan = ActionPlan(target=QualifiedName("dev", "silver", "empty"), actions=())
    results = ex.execute(empty_plan)

    assert results == ()
    assert spark.statements == []
