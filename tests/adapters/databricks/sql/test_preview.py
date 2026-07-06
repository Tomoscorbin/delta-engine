from hypothesis import given, strategies as st
import pytest

from delta_engine.adapters.databricks.sql.preview import sql_preview


def test_sql_preview_single_line_normalization_and_no_truncation() -> None:
    sql = " \nSELECT   *\nFROM  foo\tWHERE  a = 1  \n"
    out = sql_preview(sql, max_chars=10_000)
    assert out == "SELECT * FROM foo WHERE a = 1"


def test_sql_preview_truncates_and_appends_unicode_ellipsis() -> None:
    sql = "SELECT " + "x" * 300 + " FROM t"
    out = sql_preview(sql, max_chars=50)
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
    out = sql_preview(sql, max_chars=10)

    # Then it is left intact at or below the limit, and truncated only beyond it
    if truncated:
        assert out == "x" * 10 + "…"
    else:
        assert out == sql


@given(st.text(), st.integers(min_value=1, max_value=500))
def test_sql_preview_single_line_output_never_contains_newline(sql: str, max_chars: int) -> None:
    # Given: any SQL string and any max_chars
    result = sql_preview(sql, max_chars=max_chars)
    # Then: the output never contains a newline regardless of input content
    assert "\n" not in result
