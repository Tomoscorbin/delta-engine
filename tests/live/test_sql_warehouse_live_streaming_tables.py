"""
Live pins for the streaming-table facts the tags scope is built on.

A streaming table's definition — schema, comments, properties — is owned by
its pipeline; Unity Catalog tags are the one aspect durably manageable from
outside it, and only through the ALTER STREAMING TABLE dialect. Each pin
states one platform fact the engine's reader gate, validation gate, or SQL
dialect dispatch assumes.
"""

import json

import pytest

pytest.importorskip("databricks.sql")

from databricks.sql.exc import ServerOperationError

from delta_engine.adapters.databricks.sql.dialect import backtick, quote_literal
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    fetch_rows,
    live_catalog,
    live_schema,
    qualified_table,
)


def _create_streaming_table(live_connection, live_tables) -> str:
    """Create a streaming table over a one-column Delta source; skip if the workspace cannot."""
    source_name = live_tables("st_source")
    table_name = live_tables("st")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(source_name)} (id INT) USING DELTA",
    )
    try:
        execute_sql(
            live_connection,
            f"CREATE STREAMING TABLE {qualified_table(table_name)} "
            f"AS SELECT id FROM STREAM({qualified_table(source_name)})",
        )
    except Exception as exc:  # intentional broad except: environment capability probe
        pytest.skip(f"workspace cannot create a streaming table here: {exc}")
    # Plain DROP TABLE drops a streaming table (verified live), so the
    # live_tables teardown owns the cleanup; the source is dropped after it.
    return table_name


def _table_tags(live_connection, table_name: str) -> dict[str, str]:
    rows = fetch_rows(
        live_connection,
        f"SELECT tag_name, tag_value "
        f"FROM {backtick(live_catalog())}.information_schema.table_tags "
        f"WHERE schema_name = {quote_literal(live_schema())} "
        f"AND table_name = {quote_literal(table_name)}",
    )
    return {row["tag_name"]: row["tag_value"] for row in rows}


def _column_tags(live_connection, table_name: str) -> dict[tuple[str, str], str]:
    rows = fetch_rows(
        live_connection,
        f"SELECT column_name, tag_name, tag_value "
        f"FROM {backtick(live_catalog())}.information_schema.column_tags "
        f"WHERE schema_name = {quote_literal(live_schema())} "
        f"AND table_name = {quote_literal(table_name)}",
    )
    return {(row["column_name"], row["tag_name"]): row["tag_value"] for row in rows}


def test_describe_as_json_reports_the_streaming_table_kind_and_provider(
    live_connection, live_tables
):
    """DESCRIBE AS JSON reports type=STREAMING_TABLE, provider=delta for a streaming table."""
    # The admit gate in adapters/databricks/read.py is written against
    # exactly these two values. If this pin fails, the gate and the unit
    # fixtures are wrong, not this test: the assertion output carries the
    # whole document — update them to the observed values.
    table_name = _create_streaming_table(live_connection, live_tables)

    [row] = fetch_rows(
        live_connection,
        f"DESCRIBE TABLE EXTENDED {qualified_table(table_name)} AS JSON",
    )
    document = json.loads(row["json_metadata"])

    assert document.get("type") == "STREAMING_TABLE", document
    assert document.get("provider") == "delta", document


def test_alter_streaming_table_manages_table_and_column_tags(live_connection, live_tables):
    """ALTER STREAMING TABLE supports SET TAGS and UNSET TAGS at table and column level."""
    # The four tag statements are the entire surface the engine compiles
    # against a streaming table; each raises ServerOperationError on failure.
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)

    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('owner'='governance')")
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} UNSET TAGS ('owner')")
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id SET TAGS ('pii'='low')",
    )
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id UNSET TAGS ('pii')",
    )


def test_plain_alter_table_cannot_tag_a_streaming_table(live_connection, live_tables):
    """ALTER TABLE ... SET TAGS is rejected on a streaming table."""
    # The premise of the dialect dispatch (_ALTER_CLAUSES in sql/compile.py).
    # If Databricks ever starts accepting plain ALTER TABLE here, the dispatch
    # is obsolete and this pin says so.
    table_name = _create_streaming_table(live_connection, live_tables)

    with pytest.raises(ServerOperationError):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} SET TAGS ('owner'='governance')",
        )


def test_information_schema_reports_streaming_table_tags(live_connection, live_tables):
    """Tags set through ALTER STREAMING TABLE are readable from information_schema."""
    # The engine's reader observes tags via information_schema.table_tags and
    # column_tags; streaming-table tags must appear there or a tag sync could
    # never converge.
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('owner'='governance')")
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id SET TAGS ('pii'='low')",
    )

    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}
