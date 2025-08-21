from __future__ import annotations

import pytest

from tabula.adapters.databricks.sql.dialects.spark_sql import SPARK_SQL


# --- quote_identifier ---------------------------------------------------------

def test_quote_identifier_simple() -> None:
    assert SPARK_SQL.quote_identifier("orders") == "`orders`"


def test_quote_identifier_escapes_backticks() -> None:
    assert SPARK_SQL.quote_identifier("we`ird") == "`we``ird`"


def test_quote_identifier_preserves_dots_inside_single_part() -> None:
    # When quoting a single identifier, dots are treated as literal characters.
    assert SPARK_SQL.quote_identifier("a.b") == "`a.b`"


# --- quote_literal ------------------------------------------------------------

def test_quote_literal_simple() -> None:
    assert SPARK_SQL.quote_literal("abc") == "'abc'"


def test_quote_literal_escapes_single_quotes() -> None:
    assert SPARK_SQL.quote_literal("O'Reilly") == "'O''Reilly'"


# --- join_qualified_name ------------------------------------------------------

def test_join_qualified_name_uses_dot_without_requoting() -> None:
    parts = ["`c`", "`s`", "`t`"]
    assert SPARK_SQL.join_qualified_name(parts) == "`c`.`s`.`t`"


# --- render_fqn ---------------------------------------------------------------

@pytest.mark.parametrize(
    "catalog,schema,name,expected",
    [
        (None,   None,   "t",  "`t`"),
        ("c",    None,   "t",  "`c`.`t`"),
        (None,   "s",    "t",  "`s`.`t`"),
        ("c",    "s",    "t",  "`c`.`s`.`t`"),
        ("",     "s",    "t",  "`s`.`t`"),   # empty string treated as absent
        ("c",    "",     "t",  "`c`.`t`"),
    ],
)
def test_render_fqn_various(catalog, schema, name, expected) -> None:
    assert SPARK_SQL.render_qualified_name(catalog, schema, name) == expected


def test_render_fqn_escapes_each_part() -> None:
    # Backticks doubled inside any identifier part
    assert SPARK_SQL.render_qualified_name("ca`talog", "sch`ema", "na`me") == "`ca``talog`.`sch``ema`.`na``me`"
