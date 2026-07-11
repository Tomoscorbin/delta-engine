"""Shared statement-execution loop used by both Databricks backends."""

from delta_engine.adapters.databricks.errors import translate_exception
from delta_engine.adapters.databricks.execution import execute_statements
from delta_engine.application.ports import ExecutionFailed, ExecutionSucceeded


class RecordingRunner:
    """Runs statements by recording them; fails on a marker."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def __call__(self, statement: str) -> None:
        self.executed.append(statement)
        if "__nope__" in statement:
            raise Exception("boom: table not found")


def test_execute_maps_success_and_failure_without_leaking_backend_exception():
    # Given statements where the second one fails
    statements = ("SELECT 1", "SELECT * FROM __nope__")

    # When executing them
    summary = execute_statements(RecordingRunner(), statements, translate_exception)

    # Then success and failure are mapped to execution results
    results = summary.results
    assert isinstance(results[0], ExecutionSucceeded)
    assert isinstance(results[1], ExecutionFailed)
    assert results[0].statement_index == 0
    assert results[1].failure.statement_index == 1


def test_execute_failure_records_exception_details_and_the_statement():
    summary = execute_statements(
        RecordingRunner(), ("SELECT * FROM __nope__",), translate_exception
    )

    [result] = summary.results
    assert isinstance(result, ExecutionFailed)
    assert result.failure.statement_index == 0
    assert result.failure.exception_type == "Exception"
    assert "boom: table not found" in result.failure.message
    assert result.failure.statement == "SELECT * FROM __nope__"


def test_execute_records_statements_verbatim_on_results():
    # Given a statement spanning multiple lines
    statement = "ALTER TABLE t\n  ADD COLUMN x INT"

    # When executing it
    summary = execute_statements(RecordingRunner(), (statement,), translate_exception)

    # Then the result carries the SQL exactly as executed
    [result] = summary.results
    assert isinstance(result, ExecutionSucceeded)
    assert result.statement == statement


def test_execute_stops_at_first_failure_to_avoid_half_migrating():
    # Given three statements whose middle one fails
    runner = RecordingRunner()
    statements = ("SELECT 1", "SELECT * FROM __nope__", "SELECT 2")

    # When executing them
    summary = execute_statements(runner, statements, translate_exception)

    # Then the third statement is never attempted
    assert runner.executed == ["SELECT 1", "SELECT * FROM __nope__"]
    assert [type(result) for result in summary.results] == [
        ExecutionSucceeded,
        ExecutionFailed,
    ]


def test_execute_returns_empty_summary_for_no_statements():
    summary = execute_statements(RecordingRunner(), (), translate_exception)
    assert summary.results == ()
    assert summary.failed is False
