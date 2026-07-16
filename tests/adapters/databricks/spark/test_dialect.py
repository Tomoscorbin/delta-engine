from hypothesis import given, settings, strategies as st
import pytest

from delta_engine.adapters.databricks.sql.dialect import backtick, quote_literal
from tests.adapters.databricks.sql.strategies import COLUMN_NAMES, SQL_LITERAL_VALUES

pytestmark = pytest.mark.local_e2e


@settings(max_examples=25, deadline=None)
@given(st.lists(SQL_LITERAL_VALUES, min_size=1, max_size=8))
def test_quoted_literals_round_trip_through_the_spark_sql_parser(spark, values: list[str]) -> None:
    rows_sql = ", ".join(
        f"({position}, {quote_literal(value)})" for position, value in enumerate(values)
    )

    rows = spark.sql(
        f"SELECT position, value"
        f" FROM VALUES {rows_sql} AS literals(position, value)"
        f" ORDER BY position"
    ).collect()

    assert [row.value for row in rows] == values


@settings(max_examples=25, deadline=None)
@given(st.lists(COLUMN_NAMES, min_size=1, max_size=8))
def test_backticked_column_names_round_trip_through_the_spark_sql_parser(
    spark,
    names: list[str],
) -> None:
    select_list = ", ".join(
        f"{position} AS {backtick(name)}" for position, name in enumerate(names)
    )

    frame = spark.sql(f"SELECT {select_list}")

    assert frame.columns == names
