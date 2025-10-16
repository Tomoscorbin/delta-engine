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
