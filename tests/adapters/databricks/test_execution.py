"""Shared statement-execution loop used by both Databricks backends."""

from hypothesis import given, strategies as st
import pytest

from delta_engine.adapters.databricks.errors import translate_exception
from delta_engine.adapters.databricks.execution import execute_statements, sql_preview
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


def test_execute_failure_records_exception_details_and_sql_preview():
    summary = execute_statements(
        RecordingRunner(), ("SELECT * FROM __nope__",), translate_exception
    )

    [result] = summary.results
    assert isinstance(result, ExecutionFailed)
    assert result.failure.statement_index == 0
    assert result.failure.exception_type == "Exception"
    assert "boom: table not found" in result.failure.message
    assert result.failure.statement_preview == "SELECT * FROM __nope__"


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


# ----------- sql_preview: bounded single-line statement previews


def test_sql_preview_single_line_normalization_and_no_truncation():
    sql = " \nSELECT   *\nFROM  foo\tWHERE  a = 1  \n"
    assert sql_preview(sql, max_chars=10_000) == "SELECT * FROM foo WHERE a = 1"


def test_sql_preview_truncates_and_appends_unicode_ellipsis():
    sql = "SELECT " + "x" * 300 + " FROM t"
    out = sql_preview(sql, max_chars=50)
    assert out.endswith("…")
    assert out.startswith("SELECT ")


@pytest.mark.parametrize(
    ("length", "truncated"),
    [
        (9, False),  # below the limit: unchanged
        (10, False),  # exactly at the limit: unchanged (the boundary that pins <=)
        (11, True),  # one over: truncated to max_chars + ellipsis
    ],
    ids=["below", "at-limit", "over"],
)
def test_sql_preview_truncates_only_beyond_max_chars(length: int, truncated: bool):
    sql = "x" * length
    out = sql_preview(sql, max_chars=10)
    if truncated:
        assert out == "x" * 10 + "…"
    else:
        assert out == sql


@given(st.text(), st.integers(min_value=1, max_value=500))
def test_sql_preview_single_line_output_never_contains_newline(sql: str, max_chars: int):
    assert "\n" not in sql_preview(sql, max_chars=max_chars)
