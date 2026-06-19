from hypothesis import given
from hypothesis import strategies as st

from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.domain.model import QualifiedName


def test_backtick_escapes_backtick_qualified_names_and_wraps() -> None:
    assert backtick("simple") == "`simple`"
    assert backtick("we`ird") == "`we``ird`"


def test_quote_literal_escapes_single_quotes_and_wraps() -> None:
    assert quote_literal("plain") == "'plain'"
    assert quote_literal("O'Reilly") == "'O''Reilly'"


def test_backtick_qualified_name_quotes_each_part() -> None:
    qn = QualifiedName("dev", "silver", "people")
    backticked = backtick_qualified_name(qn)
    assert backticked == "`dev`.`silver`.`people`"


@given(st.text())
def test_backtick_always_wraps_with_backtick_delimiters(s: str) -> None:
    # Given: any string
    # When: backtick-quoting it
    result = backtick(s)
    # Then: output is always delimited by backticks
    assert result.startswith("`")
    assert result.endswith("`")


@given(st.text())
def test_backtick_round_trips_arbitrary_strings(s: str) -> None:
    # Given: any string
    # When: backtick-quoting then stripping delimiters and unescaping
    quoted = backtick(s)
    interior = quoted[1:-1]
    recovered = interior.replace("``", "`")
    # Then: the original string is recovered exactly
    assert recovered == s


@given(st.text())
def test_quote_literal_always_wraps_with_single_quote_delimiters(s: str) -> None:
    # Given: any string
    # When: literal-quoting it
    result = quote_literal(s)
    # Then: output is always delimited by single quotes
    assert result.startswith("'")
    assert result.endswith("'")


@given(st.text())
def test_quote_literal_round_trips_arbitrary_strings(s: str) -> None:
    # Given: any string
    # When: literal-quoting then stripping delimiters and unescaping
    quoted = quote_literal(s)
    interior = quoted[1:-1]
    recovered = interior.replace("''", "'")
    # Then: the original string is recovered exactly
    assert recovered == s
