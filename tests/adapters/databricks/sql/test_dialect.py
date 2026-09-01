from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.domain.model import QualifiedName


def test_backtick_escapes_embedded_backticks_and_wraps() -> None:
    # Then identifiers are wrapped, doubling any embedded backtick
    assert backtick("simple") == "`simple`"
    assert backtick("we`ird") == "`we``ird`"


def test_quote_literal_escapes_single_quotes_and_backslashes_and_wraps() -> None:
    # Then literals are wrapped, doubling quotes and escaping backslashes
    assert quote_literal("plain") == "'plain'"
    assert quote_literal("O'Reilly") == "'O''Reilly'"
    assert quote_literal(r"line\nbreak") == r"'line\\nbreak'"


def test_backtick_qualified_name_quotes_each_part() -> None:
    # Given a three-part qualified name
    qn = QualifiedName("dev", "silver", "people")

    # Then each part is backticked separately
    assert backtick_qualified_name(qn) == "`dev`.`silver`.`people`"
