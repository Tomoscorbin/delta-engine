"""Visitor-facing live lifecycle tests against a real Databricks SQL Warehouse."""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine
from delta_engine.schema import Column, DeltaTable, Integer, Long, String, TableProperty
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
    """Creates a table with columns, comments, tags, clustering, keys, and properties."""
    # Given a declaration using every creatable aspect
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
        properties={TableProperty.CHANGE_DATA_FEED: "true"},
        tags={"owner": "data-platform"},
    )

    # When syncing it into the live catalog
    build_sql_engine(live_connection).sync(table)

    # Then every declared aspect is observable in the catalog
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
    assert created["properties"][TableProperty.CHANGE_DATA_FEED] == "true"
    assert created["table_tags"] == {"owner": "data-platform"}
    assert created["column_tags"] == {("name", "pii"): "true"}
    assert created["primary_key"] == ("id",)
    assert isinstance(created["primary_key_name"], str) and created["primary_key_name"]


def test_sync_changes_every_mutable_table_aspect_in_live_catalog(live_connection, live_tables):
    """Evolves columns, comments, clustering, tags, and properties in a single sync."""
    # Given a synced v1 table touching every mutable aspect
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
            TableProperty.COLUMN_MAPPING_MODE: "name",
            TableProperty.CHANGE_DATA_FEED: "true",
            TableProperty.LOG_RETENTION_DURATION: "interval 15 days",
        },
        tags={"owner": "legacy-team", "obsolete": "true"},
    )
    engine = build_sql_engine(live_connection)
    engine.sync(initial)

    # When syncing a v2 declaration that changes each of those aspects
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
                comment="",
                tags={"classification": "public"},
            ),
            Column("region", String(), comment="sales region"),
            Column("age", Integer(), comment="age in years"),
        ),
        comment="",
        clustered_by=("region",),
        primary_key=("id",),
        properties={
            TableProperty.COLUMN_MAPPING_MODE: "name",
            TableProperty.CHANGE_DATA_FEED: None,
            TableProperty.LOG_RETENTION_DURATION: "interval 30 days",
            TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS: "10",
        },
        tags={"owner": "data-platform", "lifecycle": "v2"},
    )

    engine.sync(evolved)

    # Then the catalog reflects every change
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
        "",
        "sales region",
        "age in years",
    ]
    assert state["comment"] == ""
    assert state["clustering"] == ("region",)
    assert state["properties"][TableProperty.COLUMN_MAPPING_MODE] == "name"
    assert TableProperty.CHANGE_DATA_FEED not in state["properties"]
    assert state["properties"][TableProperty.LOG_RETENTION_DURATION] == "interval 30 days"
    assert state["properties"][TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS] == "10"
    assert state["table_tags"] == {"owner": "data-platform", "lifecycle": "v2"}
    assert state["column_tags"] == {("account_code", "classification"): "public"}
    assert state["primary_key"] == ("id",)
    # The reader must round-trip every aspect the executor just wrote: a
    # resync of the evolved declaration finds nothing left to do.
    assert engine.sync(evolved).has_changes is False


def test_identical_sync_does_not_alter_live_catalog(live_connection, live_tables):
    """Re-applying an identical declaration leaves the catalog byte-for-byte unchanged."""
    # Given a declaration already synced into the catalog
    table_name = live_tables("idempotent")
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer(), nullable=False), Column("name", String())),
        comment="stable declaration",
        properties={TableProperty.LOG_RETENTION_DURATION: "interval 30 days"},
        tags={"owner": "data-platform"},
    )
    engine = build_sql_engine(live_connection)
    engine.sync(table)
    before = read_live_table(live_connection, table_name)

    # When syncing the identical declaration again
    engine.sync(table)

    # Then the observed state is unchanged
    assert read_live_table(live_connection, table_name) == before


def test_dry_run_previews_creation_sql_without_creating_the_table(live_connection, live_tables):
    """A dry run previews the CREATE TABLE SQL without creating the table."""
    # Given a declaration for a table absent from the catalog
    table_name = live_tables("dry_run_create")
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer(), nullable=False),),
    )

    # When syncing as a dry run
    report = build_sql_engine(live_connection).sync(table, dry_run=True)

    # Then the CREATE TABLE SQL is previewed and the table was not created
    [statements] = report.planned_sql_statements.values()
    assert any("CREATE TABLE" in statement.upper() for statement in statements)
    assert table_exists(live_connection, table_name) is False


def test_dry_run_previews_alterations_without_changing_the_live_table(live_connection, live_tables):
    """A dry run previews ALTER SQL without changing the live table."""
    # Given an existing live table
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

    # When dry-running a declaration that adds a column and changes the comment
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

    # Then the ALTER SQL is previewed and the live table is unchanged
    [statements] = report.planned_sql_statements.values()
    joined = " ".join(statement.upper() for statement in statements)
    assert "ADD COLUMN" in joined
    assert "COMMENT ON TABLE" in joined
    assert read_live_table(live_connection, table_name) == before


def test_sync_alters_preserve_existing_rows(live_connection, live_tables):
    """Widening, adding a column, and re-clustering preserve every existing row."""
    # Given a synced table loaded with rows
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
            properties={TableProperty.TYPE_WIDENING: "true"},
        )
    )
    execute_sql(
        live_connection,
        f"INSERT INTO {qualified_table(table_name)} VALUES (1, 10), (2, 20), (3, 30)",
    )

    # When widening a populated column, adding a column, and re-layouting
    # the table — all metadata operations
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
            properties={TableProperty.TYPE_WIDENING: "true"},
        )
    )

    # Then the type widened and every loaded row survived untouched
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
    """Quoted identifiers and Unicode comments, tags, and names round-trip intact."""
    # Given a declaration full of quoting hazards: a spaced column name,
    # quotes, backslashes, and non-ASCII text in comments and tags
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
                comment=r"visitor's preferred path C:\landing\new — café",
                tags={"classification": "équipe's data"},
            ),
        ),
        comment=r"O'Reilly's live table — C:\landing\new — 東京",
        properties={TableProperty.COLUMN_MAPPING_MODE: "name"},
        tags={"owner": "data platform's team"},
    )

    # When syncing it into the live catalog
    build_sql_engine(live_connection).sync(table)

    # Then every value reads back exactly as declared
    state = read_live_table(live_connection, table_name)
    assert [column["column_name"] for column in state["columns"]] == ["id", "display name"]
    assert state["columns"][1]["comment"] == r"visitor's preferred path C:\landing\new — café"
    assert state["comment"] == r"O'Reilly's live table — C:\landing\new — 東京"
    assert state["table_tags"] == {"owner": "data platform's team"}
    assert state["column_tags"] == {("display name", "classification"): "équipe's data"}
