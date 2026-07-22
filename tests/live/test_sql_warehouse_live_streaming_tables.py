"""
Live pins for the streaming-table facts the tags scope is built on.

A streaming table's definition — schema, comments, properties — is owned by
its pipeline; Unity Catalog tags are the one aspect durably manageable from
outside it, via the documented ALTER STREAMING TABLE dialect. Each test
states platform facts the engine's reader gate, validation gate, or SQL
dialect dispatch assumes.

Two pins are deliberately absent. A raw ``DESCRIBE ... AS JSON`` pin proved
``type="STREAMING_TABLE"``, ``provider="delta"`` on every Live run and was
retired (2026-07-17): the end-to-end test carries the fact now — if the
platform changed either value, the reader's admit gate would fail that read.
And a rejection pin for plain ``ALTER TABLE ... SET TAGS`` never shipped:
the platform was observed tolerating it on a streaming table (2026-07-16);
the engine emits the documented ALTER STREAMING TABLE dialect and relies on
nothing being rejected.

Provisioning is quota-bound: the workspace tier allows one active DBSQL
pipeline at a time, so every test that creates a streaming table carries the
``streaming_table`` xdist group (serialized onto one worker by the Live
workflow's ``--dist loadgroup``) and tests share a provisioned table where
the facts allow.
"""

import time

import pytest

pytest.importorskip("databricks.sql")


from delta_engine import SyncFailedError, TableRunStatus, ValidationFailure
from delta_engine.adapters.databricks.sql.dialect import backtick, quote_literal
from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.ports import TablePresent
from delta_engine.databricks import build_sql_engine
from delta_engine.domain.model import QualifiedName, TableKind
from delta_engine.schema import Column, DeltaTable, Integer
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


def test_tags_are_the_only_aspect_the_engine_manages_on_a_streaming_table(
    live_connection, live_tables
):
    """A streaming table syncs tags to convergence; any wider scope is refused."""
    # Supersedes the old supported-relations pin that read streaming tables
    # as failed: they are now engine state, discovered — never declared. The
    # read, the round-trip, the convergence resync, and the wider-scope
    # refusal share one provisioned table (see the module docstring on the
    # pipeline quota).
    table_name = _create_streaming_table(live_connection, live_tables)
    target = qualified_table(table_name)
    execute_sql(live_connection, f"ALTER STREAMING TABLE {target} SET TAGS ('old'='remove-me')")

    reader = WarehouseReader(WarehouseSqlRunner(live_connection))
    state = reader.fetch_state(QualifiedName(live_catalog(), live_schema(), table_name))
    assert isinstance(state, TablePresent), state
    assert state.table.kind is TableKind.STREAMING_TABLE

    engine = build_sql_engine(live_connection)
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer(), tags={"pii": "low"}),),
        tags={"owner": "governance"},
        scope="tags",
    )
    engine.sync(declaration)

    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}
    # The reader must round-trip the tags the executor just wrote through the
    # ALTER STREAMING TABLE dialect: a resync finds nothing left to do.
    assert engine.sync(declaration).has_changes is False

    # Anything wider than tags is refused before planning — even when the
    # declaration mirrors the observed state exactly, so the refusal is about
    # the table's kind, not about drift.
    full_scope = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer(), tags={"pii": "low"}),),
        tags={"owner": "governance"},
    )
    with pytest.raises(SyncFailedError) as error:
        engine.sync(full_scope)

    [table_report] = error.value.report.table_reports
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert "StreamingTableTagsOnly" in {
        failure.rule_name
        for failure in table_report.failures
        if isinstance(failure, ValidationFailure)
    }
    assert table_report.planned_sql_statements == ()
    assert _table_tags(live_connection, table_name) == {"owner": "governance"}
    assert _column_tags(live_connection, table_name) == {("id", "pii"): "low"}
