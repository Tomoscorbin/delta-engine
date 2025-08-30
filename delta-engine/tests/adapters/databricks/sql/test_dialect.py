from delta_engine.adapters.databricks.sql.dialect import (
    quote_identifier,
    quote_literal,
    render_qualified_name,
)


def test_quote_identifier_escapes_backticks_and_wraps() -> None:
    assert quote_identifier("simple") == "`simple`"
    assert quote_identifier("we`ird") == "`we``ird`"


def test_quote_literal_escapes_single_quotes_and_wraps() -> None:
    assert quote_literal("plain") == "'plain'"
    assert quote_literal("O'Reilly") == "'O''Reilly'"


def test_render_qualified_name_quotes_each_part() -> None:
    assert render_qualified_name("dev", "silver", "people") == "`dev`.`silver`.`people`"


def test_render_qualified_name_escapes_backticks_in_parts() -> None:
    assert render_qualified_name("dev", "sil`ver", "peo`ple") == "`dev`.`sil``ver`.`peo``ple`"
