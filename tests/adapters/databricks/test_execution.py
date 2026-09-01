"""Databricks statement execution translates backend exceptions."""

import pytest

from delta_engine.adapters.databricks.execution import execute_statement
from delta_engine.application.errors import ExecutionError


class RecordingRunner:
    """Run statements by recording them and fail on a marker."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def __call__(self, statement: str) -> None:
        self.executed.append(statement)
        if "__nope__" in statement:
            raise Exception("boom: table not found")


def test_execute_statement_returns_normally_after_success():
    # Given a runner that records successful statements
    runner = RecordingRunner()

    # When executing a statement
    execute_statement(runner, "SELECT 1")

    # Then the statement reaches the runner unchanged
    assert runner.executed == ["SELECT 1"]


def test_execute_statement_raises_normalized_error_for_backend_failure():
    # Given a runner whose backend fails on the statement
    runner = RecordingRunner()

    with pytest.raises(ExecutionError) as exc_info:
        execute_statement(runner, "SELECT * FROM __nope__")

    # Then the failure surfaces as an execution error carrying the backend
    # exception's type and message (how those are derived is pinned in
    # test_errors.py)
    assert runner.executed == ["SELECT * FROM __nope__"]
    assert exc_info.value.exception_type == "Exception"
    assert str(exc_info.value) == "boom: table not found"
