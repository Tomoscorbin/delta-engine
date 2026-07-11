"""WarehouseExecutor: shared compilation, cursor execution, typed failures."""

from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.application.ports import ExecutionFailed, ExecutionSucceeded
from delta_engine.domain.model import Column, QualifiedName
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.plan import ActionPlan, AddColumn, DropColumn, SetTableComment

QN = QualifiedName("cat", "sch", "tbl")


class RecordingCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if "__nope__" in statement:
            raise Exception("boom: permission denied")


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_fake = RecordingCursor()

    def cursor(self):
        return self.cursor_fake


def test_executor_compiles_plan_and_executes_statements_in_order():
    connection = FakeConnection()
    executor = WarehouseExecutor(connection)
    plan = ActionPlan(actions=(AddColumn(Column("age", Integer())), DropColumn("legacy")))

    summary = executor.execute(executor.compile(QN, plan))

    assert connection.cursor_fake.executed == [
        "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT",
        "ALTER TABLE `cat`.`sch`.`tbl` DROP COLUMN `legacy`",
    ]
    assert [type(result) for result in summary.results] == [
        ExecutionSucceeded,
        ExecutionSucceeded,
    ]


def test_execute_maps_failure_and_stops_without_leaking_backend_exception():
    executor = WarehouseExecutor(FakeConnection())

    summary = executor.execute(("SELECT 1", "SELECT * FROM __nope__", "SELECT 2"))

    results = summary.results
    assert len(results) == 2  # third statement never attempted
    assert isinstance(results[0], ExecutionSucceeded)
    assert isinstance(results[1], ExecutionFailed)
    assert results[1].failure.exception_type == "Exception"
    assert "boom: permission denied" in results[1].failure.message


def test_compile_returns_the_statements_execute_would_run():
    executor = WarehouseExecutor(FakeConnection())
    plan = ActionPlan((SetTableComment(comment="hello"),))

    statements = executor.compile(QN, plan)

    assert statements == compile_plan(QN, plan)
    assert len(statements) == 1


def test_execute_of_no_statements_touches_nothing_and_reports_no_failures():
    connection = FakeConnection()
    summary = WarehouseExecutor(connection).execute(())
    assert connection.cursor_fake.executed == []
    assert summary.failed is False
