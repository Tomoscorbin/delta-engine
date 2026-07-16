"""WarehouseExecutor: shared compilation, cursor execution, typed failures."""

from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.application.ports import ExecutionFailed, ExecutionSucceeded
from delta_engine.domain.model import DesiredColumn, ObservedColumn, QualifiedName, TableKind
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.plan import ActionPlan, AddColumn, DropColumn, SetTableComment

QN = QualifiedName("cat", "sch", "tbl")


class RecordingCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.closed = False
        self.close_raises = False

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if "__nope__" in statement:
            raise Exception("boom: permission denied")

    def close(self) -> None:
        self.closed = True
        if self.close_raises:
            raise Exception("network dropped while closing")


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_fake = RecordingCursor()
        self.cursor_requests = 0

    def cursor(self):
        self.cursor_requests += 1
        return self.cursor_fake


class ClosedConnection:
    """Connection whose cursor acquisition fails, like a closed/expired session."""

    def cursor(self):
        raise Exception("cannot create cursor from closed connection")


def test_executor_compiles_plan_and_executes_statements_in_order():
    connection = FakeConnection()
    executor = WarehouseExecutor(connection)
    plan = ActionPlan(
        actions=(
            AddColumn(DesiredColumn("age", Integer())),
            DropColumn(ObservedColumn("legacy", Integer())),
        )
    )

    summary = executor.execute(executor.compile(QN, plan, TableKind.TABLE))

    assert connection.cursor_fake.executed == [
        "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT",
        "ALTER TABLE `cat`.`sch`.`tbl` DROP COLUMN `legacy`",
    ]
    assert [type(result) for result in summary.results] == [
        ExecutionSucceeded,
        ExecutionSucceeded,
    ]
    assert connection.cursor_fake.closed is True


def test_execute_maps_failure_and_stops_without_leaking_backend_exception():
    executor = WarehouseExecutor(FakeConnection())

    summary = executor.execute(("SELECT 1", "SELECT * FROM __nope__", "SELECT 2"))

    results = summary.results
    assert len(results) == 2  # third statement never attempted
    assert isinstance(results[0], ExecutionSucceeded)
    assert isinstance(results[1], ExecutionFailed)
    assert results[1].failure.exception_type == "Exception"
    assert "boom: permission denied" in results[1].failure.message


def test_execute_contains_cursor_acquisition_failure_as_a_result():
    # Given a connection that cannot produce a cursor
    executor = WarehouseExecutor(ClosedConnection())

    # When executing a plan's statements
    summary = executor.execute(("SELECT 1", "SELECT 2"))

    # Then the failure is returned through the summary, not raised
    [result] = summary.results
    assert isinstance(result, ExecutionFailed)
    assert result.failure.statement_index == 0
    assert "closed connection" in result.failure.message
    assert result.failure.statement == "SELECT 1"


def test_execute_keeps_the_summary_when_cursor_close_fails():
    # Given a cursor that raises while closing, after a clean run
    connection = FakeConnection()
    connection.cursor_fake.close_raises = True

    # When executing a statement
    summary = WarehouseExecutor(connection).execute(("SELECT 1",))

    # Then the successful summary survives the close failure
    assert summary.failed is False
    assert [type(result) for result in summary.results] == [ExecutionSucceeded]


def test_compile_returns_the_statements_execute_would_run():
    executor = WarehouseExecutor(FakeConnection())
    plan = ActionPlan((SetTableComment(desired_comment="hello", observed_comment=""),))

    statements = executor.compile(QN, plan, TableKind.TABLE)

    assert statements == compile_plan(QN, plan, TableKind.TABLE)
    assert len(statements) == 1


def test_execute_of_no_statements_touches_nothing_and_reports_no_failures():
    connection = FakeConnection()
    summary = WarehouseExecutor(connection).execute(())
    assert connection.cursor_requests == 0
    assert summary.failed is False
