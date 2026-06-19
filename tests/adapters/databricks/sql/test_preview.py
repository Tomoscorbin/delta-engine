from types import SimpleNamespace

from hypothesis import given, strategies as st
from py4j.protocol import Py4JJavaError
import pytest

from delta_engine.adapters.databricks.sql.preview import (
    error_preview,
    exc_type_name,
    sql_preview,
)


def test_sql_preview_single_line_normalization_and_no_truncation() -> None:
    sql = " \nSELECT   *\nFROM  foo\tWHERE  a = 1  \n"
    out = sql_preview(sql, max_chars=10_000, single_line=True)
    assert out == "SELECT * FROM foo WHERE a = 1"


def test_sql_preview_preserves_internal_whitespace_when_single_line_false() -> None:
    sql = "SELECT   *\nFROM  foo\nWHERE a = 1"
    out = sql_preview(sql, max_chars=10_000, single_line=False)
    # Leading/trailing stripped, but internal newlines/spaces kept
    assert out.startswith("SELECT   *\nFROM  foo")
    assert "\nWHERE a = 1" in out


def test_sql_preview_truncates_and_appends_unicode_ellipsis() -> None:
    sql = "SELECT " + "x" * 300 + " FROM t"
    out = sql_preview(sql, max_chars=50, single_line=True)
    assert out.endswith("…")
    assert len(out) > 50  # because the ellipsis is appended after slicing
    # sanity: prefix preserved
    assert out.startswith("SELECT ")


@pytest.mark.parametrize(
    "length, truncated",
    [
        (9, False),  # below the limit: unchanged
        (10, False),  # exactly at the limit: unchanged (the boundary that pins <=)
        (11, True),  # one over: truncated to max_chars + ellipsis
    ],
    ids=["below", "at-limit", "over"],
)
def test_sql_preview_truncates_only_beyond_max_chars(length: int, truncated: bool) -> None:
    # Given a single-line SQL string of a known length around max_chars=10
    sql = "x" * length

    # When previewing it with max_chars=10
    out = sql_preview(sql, max_chars=10, single_line=True)

    # Then it is left intact at or below the limit, and truncated only beyond it
    if truncated:
        assert out == "x" * 10 + "…"
    else:
        assert out == sql


def test_error_preview_returns_first_5_lines() -> None:
    msg = "L1\nL2\nL3\nL4\nL5\nL6\nL7"
    exc = Exception(msg)
    out = error_preview(exc)
    assert out == "L1\nL2\nL3\nL4\nL5"


def test_error_preview_short_message_unchanged() -> None:
    exc = Exception("Only one line")
    assert error_preview(exc) == "Only one line"


def test_exc_type_name_reports_underlying_java_class_for_py4j_error() -> None:
    # Given a Py4JJavaError whose java_exception reports a known Java class
    java_exception = SimpleNamespace(
        _target_id="o1",
        getClass=lambda: SimpleNamespace(getName=lambda: "org.apache.spark.sql.AnalysisException"),
    )
    error = Py4JJavaError("boom", java_exception)

    # When naming the exception type
    name = exc_type_name(error)

    # Then the underlying Java class is returned, not the py4j wrapper name
    assert name == "org.apache.spark.sql.AnalysisException"


def test_exc_type_name_returns_python_class_for_plain_exception() -> None:
    # Given a plain Python exception (no JVM origin)
    # Then the Python class name is used directly
    assert exc_type_name(ValueError("nope")) == "ValueError"


@given(st.text(), st.integers(min_value=1, max_value=500))
def test_sql_preview_single_line_output_never_contains_newline(sql: str, max_chars: int) -> None:
    # Given: any SQL string and any max_chars
    # When: previewing with single_line=True
    result = sql_preview(sql, max_chars=max_chars, single_line=True)
    # Then: the output never contains a newline regardless of input content
    assert "\n" not in result
