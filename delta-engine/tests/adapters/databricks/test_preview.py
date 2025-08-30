from delta_engine.adapters.databricks.preview import (
    error_preview,
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


def test_error_preview_returns_first_5_lines() -> None:
    msg = "L1\nL2\nL3\nL4\nL5\nL6\nL7"
    exc = Exception(msg)
    out = error_preview(exc)
    assert out == "L1\nL2\nL3\nL4\nL5"


def test_error_preview_short_message_unchanged() -> None:
    exc = Exception("Only one line")
    assert error_preview(exc) == "Only one line"
