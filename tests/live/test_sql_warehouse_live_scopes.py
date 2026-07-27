"""Live proofs that restricted scopes change only the catalog aspects they own."""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine
from delta_engine.schema import Column, DeltaTable, Integer, String, Timestamp
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
    read_live_table,
)


def test_metadata_scope_changes_metadata_and_preserves_external_ddl(live_connection, live_tables):
    """A metadata-scoped sync updates governed metadata but preserves external structure."""
    table_name = live_tables("metadata_scope")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} "
        "(id INT NOT NULL COMMENT 'external id', payload STRING) USING DELTA "
        "COMMENT 'external comment' "
        "TBLPROPERTIES ('delta.logRetentionDuration'='interval 10 days') "
        "CLUSTER BY (id)",
    )

    build_sql_engine(live_connection).sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("id", Integer(), nullable=False, comment="governed id"),
                Column("payload", String(), comment="governed payload"),
            ),
            comment="governed metadata",
            tags={"domain": "sales"},
            clustered_by=("id",),
            primary_key=("id",),
            scope="metadata",
        )
    )

    state = read_live_table(live_connection, table_name)
    assert [(column["column_name"], column["full_data_type"]) for column in state["columns"]] == [
        ("id", "int"),
        ("payload", "string"),
    ]
    assert [column["comment"] for column in state["columns"]] == [
        "governed id",
        "governed payload",
    ]
    assert state["comment"] == "governed metadata"
    assert state["table_tags"] == {"domain": "sales"}
    assert state["primary_key"] == ("id",)
    assert state["clustering"] == ("id",)
    assert state["properties"]["delta.logRetentionDuration"] == "interval 10 days"


def test_tag_scope_changes_only_table_and_column_tags(live_connection, live_tables):
    """A tags-scoped sync changes only table and column tags, leaving all else intact."""
    table_name = live_tables("tag_scope")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} "
        "(id INT NOT NULL COMMENT 'external id', payload STRING) USING DELTA "
        "COMMENT 'externally owned table' "
        "TBLPROPERTIES ('delta.logRetentionDuration'='interval 12 days')",
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(table_name)} SET TAGS ('old'='remove-me')",
    )

    before = read_live_table(live_connection, table_name)
    build_sql_engine(live_connection).sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                # Unmanaged aspects (structure, comments) must mirror the live
                # table — drift outside the tags scope fails validation.
                Column(
                    "id",
                    Integer(),
                    nullable=False,
                    comment="external id",
                    tags={"identifier": "true"},
                ),
                Column("payload", String(), tags={"classification": "internal"}),
            ),
            comment="externally owned table",
            tags={"owner": "governance"},
            scope="tags",
        )
    )

    state = read_live_table(live_connection, table_name)
    assert state["table_tags"] == {"owner": "governance"}
    assert state["column_tags"] == {
        ("id", "identifier"): "true",
        ("payload", "classification"): "internal",
    }
    assert state["columns"] == before["columns"]
    assert state["comment"] == before["comment"]
    assert state["properties"] == before["properties"]
    assert state["primary_key"] == before["primary_key"]
    assert state["clustering"] == before["clustering"]
    assert state["partitioning"] == before["partitioning"]


def test_metadata_scope_accepts_camel_case_display_names_without_drift(
    live_connection, live_tables
):
    """A metadata-scoped sync matches camelCase display names without structural drift."""
    table_name = live_tables("column_case_metadata_sync")
    # Given a live table created with camelCase display names
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} ("
        "invocation_timestamp TIMESTAMP, "
        "`requestId` STRING NOT NULL, "
        "user_email STRING, "
        "model_short STRING, "
        "hour INT, "
        "session_id STRING, "
        "operation STRING, "
        "input_tokens INT, "
        "cache_read_tokens INT, "
        "cache_write_tokens INT, "
        "output_tokens INT, "
        "bedrock_account_id STRING, "
        "bedrock_aws_region STRING, "
        "`modelId` STRING, "
        f"CONSTRAINT {table_name}_pk PRIMARY KEY (`requestId`)"
        ") USING DELTA "
        "CLUSTER BY (invocation_timestamp, `requestId`, user_email, model_short)",
    )

    declaration = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=table_name,
        columns=(
            Column("invocation_timestamp", Timestamp()),
            Column(
                "requestId",
                String(),
                nullable=False,
                comment="AWS request identifier",
            ),
            Column("user_email", String()),
            Column("model_short", String()),
            Column("hour", Integer()),
            Column("session_id", String()),
            Column("operation", String()),
            Column("input_tokens", Integer()),
            Column("cache_read_tokens", Integer()),
            Column("cache_write_tokens", Integer()),
            Column("output_tokens", Integer()),
            Column("bedrock_account_id", String()),
            Column("bedrock_aws_region", String()),
            Column("modelId", String(), comment="Full Bedrock model identifier"),
        ),
        comment="Bedrock request invocations",
        clustered_by=(
            "invocation_timestamp",
            "requestId",
            "user_email",
            "model_short",
        ),
        primary_key=("requestId",),
        scope="metadata",
    )
    engine = build_sql_engine(live_connection)

    # When syncing a metadata-scoped declaration matching those spellings
    report = engine.sync(declaration)

    # Then comments apply through the camelCase spellings without structural drift
    assert report.has_failures is False
    assert report.has_changes is True
    statements = next(iter(report.planned_sql_statements.values()))
    assert any("ALTER COLUMN `requestId` COMMENT" in statement for statement in statements)
    assert any("ALTER COLUMN `modelId` COMMENT" in statement for statement in statements)

    state = read_live_table(live_connection, table_name)
    columns = {column["column_name"]: column for column in state["columns"]}
    assert columns["requestId"]["comment"] == "AWS request identifier"
    assert columns["modelId"]["comment"] == "Full Bedrock model identifier"
    assert state["comment"] == "Bedrock request invocations"
    assert engine.sync(declaration).has_changes is False
