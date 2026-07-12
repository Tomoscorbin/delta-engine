"""Visitor-facing live lifecycle tests against a real Databricks SQL Warehouse."""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine
from delta_engine.schema import Column, DeltaTable, Integer, Long, Property, String
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    fetch_rows,
    live_catalog,
    live_schema,
    qualified_table,
    read_live_table,
    table_exists,
)


def test_sync_builds_feature_rich_table_in_live_catalog(live_connection, live_tables):
    table_name = live_tables("create")
    table = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=table_name,
        columns=(
            Column("id", Integer(), nullable=False, comment="stable identifier"),
            Column("name", String(), comment="customer name", tags={"pii": "true"}),
        ),
        comment="live customer table",
        clustered_by=("id",),
        primary_key=("id",),
        properties={Property.CHANGE_DATA_FEED: "true"},
        tags={"owner": "data-platform"},
    )

    build_sql_engine(live_connection).sync(table)

    created = read_live_table(live_connection, table_name)
    assert created["format"] == "delta"
    assert [column["column_name"] for column in created["columns"]] == ["id", "name"]
    assert [column["full_data_type"].casefold() for column in created["columns"]] == [
        "int",
        "string",
    ]
    assert [column["is_nullable"] for column in created["columns"]] == ["NO", "YES"]
    assert [column["comment"] for column in created["columns"]] == [
        "stable identifier",
        "customer name",
    ]
    assert created["comment"] == "live customer table"
    assert created["clustering"] == ("id",)
    assert created["properties"][Property.CHANGE_DATA_FEED] == "true"
    assert created["table_tags"] == {"owner": "data-platform"}
    assert created["column_tags"] == {("name", "pii"): "true"}
    assert created["primary_key"] == ("id",)
    assert created["primary_key_name"] == f"{table_name}_pk"


def test_sync_changes_every_mutable_table_aspect_in_live_catalog(live_connection, live_tables):
    table_name = live_tables("lifecycle")
    initial = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column(
                "account_code",
                String(),
                nullable=False,
                comment="legacy account code",
                tags={"classification": "internal", "obsolete": "true"},
            ),
            Column("region", String()),
            Column("legacy", String(), comment="removed by v2"),
        ),
        comment="customer schema v1",
        clustered_by=("id",),
        primary_key=("id",),
        properties={
            Property.COLUMN_MAPPING_MODE: "name",
            Property.CHANGE_DATA_FEED: "true",
            Property.LOG_RETENTION_DURATION: "interval 15 days",
        },
        tags={"owner": "legacy-team", "obsolete": "true"},
    )
    engine = build_sql_engine(live_connection)
    engine.sync(initial)

    evolved = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("id", Integer(), nullable=False, comment="stable customer id"),
            Column(
                "account_code",
                String(),
                nullable=True,
                # Clearing a column comment to "" is deliberately not
                # exercised: it compiles to ALTER COLUMN ... UNSET COMMENT,
                # which SQL warehouses reject — see the todo on the pending
                # COMMENT '' compiler fix.
                comment="code retained for audit",
                tags={"classification": "public"},
            ),
            Column("region", String(), comment="sales region"),
            Column("age", Integer(), comment="age in years"),
        ),
        comment="",
        clustered_by=("region",),
        primary_key=("id",),
        properties={
            Property.COLUMN_MAPPING_MODE: "name",
            Property.CHANGE_DATA_FEED: None,
            Property.LOG_RETENTION_DURATION: "interval 30 days",
            Property.DATA_SKIPPING_NUM_INDEXED_COLS: "10",
        },
        tags={"owner": "data-platform", "lifecycle": "v2"},
    )

    engine.sync(evolved)

    state = read_live_table(live_connection, table_name)
    assert [column["column_name"] for column in state["columns"]] == [
        "id",
        "account_code",
        "region",
        "age",
    ]
    assert [column["is_nullable"] for column in state["columns"]] == [
        "NO",
        "YES",
        "YES",
        "YES",
    ]
    assert [column["comment"] for column in state["columns"]] == [
        "stable customer id",
        "code retained for audit",
        "sales region",
        "age in years",
    ]
    assert state["comment"] == ""
    assert state["clustering"] == ("region",)
    assert state["properties"][Property.COLUMN_MAPPING_MODE] == "name"
    assert Property.CHANGE_DATA_FEED not in state["properties"]
    assert state["properties"][Property.LOG_RETENTION_DURATION] == "interval 30 days"
    assert state["properties"][Property.DATA_SKIPPING_NUM_INDEXED_COLS] == "10"
    assert state["table_tags"] == {"owner": "data-platform", "lifecycle": "v2"}
    assert state["column_tags"] == {("account_code", "classification"): "public"}
    assert state["primary_key"] == ("id",)
    # The reader must round-trip every aspect the executor just wrote: a
    # resync of the evolved declaration finds nothing left to do.
    assert engine.sync(evolved).has_changes is False


def test_identical_sync_does_not_alter_live_catalog(live_connection, live_tables):
    table_name = live_tables("idempotent")
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer(), nullable=False), Column("name", String())),
        comment="stable declaration",
        properties={Property.LOG_RETENTION_DURATION: "interval 30 days"},
        tags={"owner": "data-platform"},
    )
    engine = build_sql_engine(live_connection)
    engine.sync(table)
    before = read_live_table(live_connection, table_name)

    engine.sync(table)

    assert read_live_table(live_connection, table_name) == before


def test_dry_run_previews_creation_sql_without_creating_the_table(live_connection, live_tables):
    table_name = live_tables("dry_run_create")
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer(), nullable=False),),
    )

    report = build_sql_engine(live_connection).sync(table, dry_run=True)

    [statements] = report.planned_sql_statements.values()
    assert any("CREATE TABLE" in statement.upper() for statement in statements)
    assert table_exists(live_connection, table_name) is False


def test_dry_run_previews_alterations_without_changing_the_live_table(live_connection, live_tables):
    table_name = live_tables("dry_run_alter")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer(), nullable=False), Column("name", String())),
            comment="before the dry run",
        )
    )
    before = read_live_table(live_connection, table_name)

    report = engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("name", String()),
                Column("age", Integer()),
            ),
            comment="after the dry run",
        ),
        dry_run=True,
    )

    [statements] = report.planned_sql_statements.values()
    joined = " ".join(statement.upper() for statement in statements)
    assert "ADD COLUMN" in joined
    assert "COMMENT ON TABLE" in joined
    assert read_live_table(live_connection, table_name) == before


def test_sync_alters_preserve_existing_rows(live_connection, live_tables):
    table_name = live_tables("data_preserved")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("amount", Integer()),
            ),
            properties={Property.TYPE_WIDENING: "true"},
        )
    )
    execute_sql(
        live_connection,
        f"INSERT INTO {qualified_table(table_name)} VALUES (1, 10), (2, 20), (3, 30)",
    )

    # Widening a populated column, adding a column, and re-layouting the
    # table are metadata operations: the loaded rows must survive untouched.
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("amount", Long()),
                Column("note", String(), comment="added after load"),
            ),
            clustered_by=("id",),
            properties={Property.TYPE_WIDENING: "true"},
        )
    )

    state = read_live_table(live_connection, table_name)
    assert state["columns"][1]["full_data_type"].casefold() == "bigint"
    rows = fetch_rows(
        live_connection,
        f"SELECT id, amount, note FROM {qualified_table(table_name)} ORDER BY id",
    )
    assert rows == [
        {"id": 1, "amount": 10, "note": None},
        {"id": 2, "amount": 20, "note": None},
        {"id": 3, "amount": 30, "note": None},
    ]


def test_sync_round_trips_quoted_identifiers_and_unicode_metadata(live_connection, live_tables):
    table_name = live_tables("quoted")
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("id", Integer()),
            Column(
                "display name",
                String(),
                comment="visitor's preferred name — café",
                tags={"classification": "équipe's data"},
            ),
        ),
        comment="O'Reilly's live table — 東京",
        properties={Property.COLUMN_MAPPING_MODE: "name"},
        tags={"owner": "data platform's team"},
    )

    build_sql_engine(live_connection).sync(table)

    state = read_live_table(live_connection, table_name)
    assert [column["column_name"] for column in state["columns"]] == ["id", "display name"]
    assert state["columns"][1]["comment"] == "visitor's preferred name — café"
    assert state["comment"] == "O'Reilly's live table — 東京"
    assert state["table_tags"] == {"owner": "data platform's team"}
    assert state["column_tags"] == {("display name", "classification"): "équipe's data"}
