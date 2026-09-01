"""WarehouseExecutor compiles plans and contains each cursor execution."""

import pytest

from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.application.errors import ExecutionError
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan, SetTableComment
from tests.adapters.databricks.fakes import ClosedConnection

QN = QualifiedName("cat", "sch", "tbl")


def _executor(connection) -> WarehouseExecutor:
    return WarehouseExecutor(WarehouseSqlRunner(connection))


class RecordingCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.closed = False

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if "__nope__" in statement:
            raise Exception("boom: permission denied")

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_fake = RecordingCursor()
        self.cursor_requests = 0

    def cursor(self):
        self.cursor_requests += 1
        return self.cursor_fake


def test_execute_runs_one_statement_and_closes_its_cursor():
    # Given one statement to run
    connection = FakeConnection()

    result = _executor(connection).execute("SELECT 1")

    # Then it runs on its own cursor, which is closed afterwards
    assert result is None
    assert connection.cursor_requests == 1
    assert connection.cursor_fake.executed == ["SELECT 1"]
    assert connection.cursor_fake.closed is True


def test_execute_translates_statement_failure_and_closes_cursor():
    # Given a statement the backend rejects
    connection = FakeConnection()

    with pytest.raises(ExecutionError) as exc_info:
        _executor(connection).execute("SELECT * FROM __nope__")

    # Then the failure surfaces as an execution error and the cursor is closed
    assert exc_info.value.exception_type == "Exception"
    assert "boom: permission denied" in str(exc_info.value)
    assert connection.cursor_fake.closed is True


def test_execute_translates_cursor_acquisition_failure():
    # Given a connection that cannot open a cursor
    executor = _executor(ClosedConnection())

    with pytest.raises(ExecutionError) as exc_info:
        executor.execute("SELECT 1")

    # Then the acquisition failure is an execution error too
    assert exc_info.value.exception_type == "RuntimeError"
    assert "closed connection" in str(exc_info.value)


def test_compile_returns_backend_statements_without_touching_connection():
    # Given a one-action plan
    connection = FakeConnection()
    executor = _executor(connection)
    plan = ActionPlan(
        target=QN,
        actions=(SetTableComment(desired_comment="hello", observed_comment=""),),
    )

    compiled = executor.compile(plan)

    # Then compilation matches the SQL compiler and opens no cursor
    assert compiled == compile_plan(plan)
    assert len(compiled.statements) == 1
    assert connection.cursor_requests == 0


def test_compile_of_empty_plan_returns_no_statements():
    # Given an empty plan
    connection = FakeConnection()

    compiled = _executor(connection).compile(ActionPlan(target=QN))

    # Then nothing is compiled and no cursor is opened
    assert compiled.statements == ()
    assert connection.cursor_requests == 0
