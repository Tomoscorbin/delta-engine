"""
Live pins for the streaming-table facts the tags scope is built on.

A streaming table's definition — schema, comments, properties — is owned by
its pipeline; Unity Catalog tags are the one aspect durably manageable from
outside it, and only through the ALTER STREAMING TABLE dialect. Each pin
states platform facts the engine's reader gate, validation gate, or SQL
dialect dispatch assumes.

Provisioning is quota-bound: the workspace tier allows one active DBSQL
pipeline at a time, so every test that creates a streaming table carries the
``streaming_table`` xdist group (serialized onto one worker by the Live
workflow's ``--dist loadgroup``) and pins share a provisioned table where the
facts allow.
"""

import json
import time

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

pytestmark = pytest.mark.xdist_group("streaming_table")

_QUOTA_ERROR_MARKER = "QUOTA_EXCEEDED_EXCEPTION"
_QUOTA_RETRY_ATTEMPTS = 6
_QUOTA_RETRY_WAIT_SECONDS = 20


def _create_streaming_table(live_connection, live_tables) -> str:
    """Create a streaming table over a one-column Delta source; skip if the workspace cannot."""
    source_name = live_tables("st_source")
    table_name = live_tables("st")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(source_name)} (id INT) USING DELTA",
    )
    create_statement = (
        f"CREATE STREAMING TABLE {qualified_table(table_name)} "
        f"AS SELECT id FROM STREAM({qualified_table(source_name)})"
    )
    # The one-pipeline quota releases asynchronously after a previous test's
    # DROP TABLE, so a quota rejection is retried before concluding the
    # workspace cannot create streaming tables at all. Plain DROP TABLE drops
    # a streaming table (verified live), so the live_tables teardown owns the
    # cleanup; the source is dropped after it.
    attempt = 1
    while True:
        try:
            execute_sql(live_connection, create_statement)
            return table_name
        except Exception as exc:  # intentional broad except: environment capability probe
            if _QUOTA_ERROR_MARKER in str(exc) and attempt < _QUOTA_RETRY_ATTEMPTS:
                attempt += 1
                time.sleep(_QUOTA_RETRY_WAIT_SECONDS)
                continue
            pytest.skip(f"workspace cannot create a streaming table here: {exc}")


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


def test_a_streaming_table_reports_its_kind_and_rejects_plain_alter_table(
    live_connection, live_tables
):
    """DESCRIBE AS JSON reports type=STREAMING_TABLE, provider=delta; ALTER TABLE is rejected."""
    # Two facts on one provisioned table. First: the admit gate in
    # adapters/databricks/read.py is written against exactly the type and
    # provider values asserted here — on failure the assertion output carries
    # the whole document; update the gate and the unit fixtures to the
    # observed values, not this test. Second: plain ALTER TABLE ... SET TAGS
    # is rejected — the premise of the dialect dispatch (_ALTER_CLAUSES in
    # sql/compile.py). If Databricks ever accepts it, the dispatch is
    # obsolete and this pin says so.
    table_name = _create_streaming_table(live_connection, live_tables)

    [row] = fetch_rows(
        live_connection,
        f"DESCRIBE TABLE EXTENDED {qualified_table(table_name)} AS JSON",
    )
    document = json.loads(row["json_metadata"])
    assert document.get("type") == "STREAMING_TABLE", document
    assert document.get("provider") == "delta", document

    with pytest.raises(ServerOperationError):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} SET TAGS ('owner'='governance')",
        )


def test_alter_streaming_table_tags_round_through_information_schema(live_connection, live_tables):
    """ALTER STREAMING TABLE manages table and column tags, visible in information_schema."""
    # The four tag statements are the entire surface the engine compiles
    # against a streaming table, and information_schema.table_tags /
    # column_tags is where the engine's reader observes them — both facts on
    # one provisioned table, with each statement's effect asserted.
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)

    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('owner'='governance')")
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id SET TAGS ('pii'='low')",
    )
    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}

    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} UNSET TAGS ('owner')")
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {target} ALTER COLUMN id UNSET TAGS ('pii')",
    )
    assert _table_tags(live_connection, table_name) == {}
    assert _column_tags(live_connection, table_name) == {}
