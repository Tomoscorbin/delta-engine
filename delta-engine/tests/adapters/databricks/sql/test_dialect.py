from delta_engine.adapters.databricks.sql.dialect import (
    quote_identifier,
    quote_literal,
    quote_qualified_name,
)
from tests.factories import make_qualified_name


def test_quote_identifier_escapes_backticks_and_wraps() -> None:
    assert quote_identifier("simple") == "`simple`"
    assert quote_identifier("we`ird") == "`we``ird`"


def test_quote_literal_escapes_single_quotes_and_wraps() -> None:
    assert quote_literal("plain") == "'plain'"
    assert quote_literal("O'Reilly") == "'O''Reilly'"


def test_quote_qualified_name_quotes_each_part() -> None:
    qn = make_qualified_name("dev", "silver", "people")
    quoted = quote_qualified_name(qn)
    assert quoted == "`dev`.`silver`.`people`"
