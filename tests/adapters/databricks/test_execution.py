"""Databricks statement execution translates backend exceptions."""

from types import SimpleNamespace

import pytest

from delta_engine.adapters.databricks.execution import execute_statement
from delta_engine.application.ports import ExecutionError


class RecordingRunner:
    """Run statements by recording them and fail on a marker."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def __call__(self, statement: str) -> None:
        self.executed.append(statement)
        if "__nope__" in statement:
            raise Exception("boom: table not found")


def test_execute_statement_returns_normally_after_success():
    runner = RecordingRunner()

    result = execute_statement(runner, "SELECT 1")

    assert result is None
    assert runner.executed == ["SELECT 1"]


def test_execute_statement_raises_normalized_error_for_backend_failure():
    runner = RecordingRunner()

    with pytest.raises(ExecutionError) as exc_info:
        execute_statement(runner, "SELECT * FROM __nope__")

    assert runner.executed == ["SELECT * FROM __nope__"]
    assert exc_info.value.exception_type == "Exception"
    assert str(exc_info.value) == "boom: table not found"


def test_execute_statement_contains_an_exception_with_an_unrenderable_message():
    class UnrenderableError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("rendering failed")

    def fail(_statement: str) -> None:
        raise UnrenderableError()

    with pytest.raises(ExecutionError) as exc_info:
        execute_statement(fail, "SELECT 1")

    assert exc_info.value.exception_type == "UnrenderableError"
    assert str(exc_info.value) == "<exception message unavailable>"


def test_execute_statement_names_wrapped_jvm_exception_by_java_class():
    class WrappingRunner:
        def __call__(self, _statement: str) -> None:
            error = Exception("boom")
            error.java_exception = SimpleNamespace(  # type: ignore[attr-defined]
                getClass=lambda: SimpleNamespace(
                    getName=lambda: "org.apache.spark.sql.AnalysisException"
                )
            )
            raise error

    with pytest.raises(ExecutionError) as exc_info:
        execute_statement(WrappingRunner(), "SELECT 1")

    assert exc_info.value.exception_type == "org.apache.spark.sql.AnalysisException"
