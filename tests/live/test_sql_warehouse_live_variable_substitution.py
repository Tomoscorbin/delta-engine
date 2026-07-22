"""Live proof that SQL Warehouses preserve Spark-style variable expressions."""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine
from delta_engine.schema import Column, DeltaTable, Integer
from tests.live.sql_warehouse_live_helpers import (
    fetch_rows,
    live_catalog,
    live_schema,
    read_live_table,
)


def test_sql_warehouse_preserves_spark_style_variable_expressions(
    live_connection,
    live_tables,
):
    """`${...}` is literal through both the connector and a convergent engine sync."""
    expression = "${env:HOME}"

    assert fetch_rows(live_connection, f"SELECT '{expression}' AS value") == [{"value": expression}]

    table_name = live_tables("variable_expression")
    comment = f"documentation containing {expression}"
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column(
                "id",
                Integer(),
                comment=comment,
                tags={"substitution_probe": expression},
            ),
        ),
        comment=comment,
        tags={"substitution_probe": expression},
    )
    engine = build_sql_engine(live_connection)

    first_report = engine.sync(declaration)

    assert first_report.has_failures is False
    state = read_live_table(live_connection, table_name)
    assert state["comment"] == comment
    assert state["columns"][0]["comment"] == comment
    assert state["table_tags"] == {"substitution_probe": expression}
    assert state["column_tags"] == {("id", "substitution_probe"): expression}

    assert engine.sync(declaration).has_changes is False
