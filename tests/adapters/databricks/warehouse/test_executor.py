"""WarehouseExecutor compiles plans and contains each cursor execution."""

import pytest

from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.application.errors import ExecutionError
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan, SetTableComment

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
    """Connection whose cursor acquisition fails, like a closed session."""

    def cursor(self):
        raise Exception("cannot create cursor from closed connection")


def test_execute_runs_one_statement_and_closes_its_cursor():
    connection = FakeConnection()

    result = WarehouseExecutor(connection).execute("SELECT 1")

    assert result is None
    assert connection.cursor_requests == 1
    assert connection.cursor_fake.executed == ["SELECT 1"]
    assert connection.cursor_fake.closed is True


def test_execute_translates_statement_failure_and_closes_cursor():
    connection = FakeConnection()

    with pytest.raises(ExecutionError) as exc_info:
        WarehouseExecutor(connection).execute("SELECT * FROM __nope__")

    assert exc_info.value.exception_type == "Exception"
    assert "boom: permission denied" in str(exc_info.value)
    assert connection.cursor_fake.closed is True


def test_execute_translates_cursor_acquisition_failure():
    executor = WarehouseExecutor(ClosedConnection())

    with pytest.raises(ExecutionError) as exc_info:
        executor.execute("SELECT 1")

    assert exc_info.value.exception_type == "Exception"
    assert "closed connection" in str(exc_info.value)


def test_execute_suppresses_cursor_close_failure_after_success():
    connection = FakeConnection()
    connection.cursor_fake.close_raises = True

    result = WarehouseExecutor(connection).execute("SELECT 1")

    assert result is None
    assert connection.cursor_fake.closed is True


def test_compile_returns_backend_statements_without_touching_connection():
    connection = FakeConnection()
    executor = WarehouseExecutor(connection)
    plan = ActionPlan((SetTableComment(desired_comment="hello", observed_comment=""),))

    statements = executor.compile(QN, plan)

    assert statements == compile_plan(QN, plan)
    assert len(statements) == 1
    assert connection.cursor_requests == 0


def test_compile_of_empty_plan_returns_no_statements():
    connection = FakeConnection()

    statements = WarehouseExecutor(connection).compile(QN, ActionPlan())

    assert statements == ()
    assert connection.cursor_requests == 0
