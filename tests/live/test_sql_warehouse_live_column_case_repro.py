"""
Throwaway live reproductions for camelCase column identifier handling.

These tests isolate the case-sensitivity theory behind a metadata-scoped sync
that reported ``UnmanagedAspectDrift`` for an otherwise matching live schema.
Remove this module once the live run has established the platform behaviour.
"""

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


def test_column_identifier_case_repro_raw_alter_uses_lowercase_reference(
    live_connection, live_tables
):
    """ALTER COLUMN resolves a lowercase identifier to a camelCase live column."""
    table_name = live_tables("column_case_raw_alter")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (`requestId` STRING) USING DELTA",
    )

    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(table_name)} "
        "ALTER COLUMN `requestid` COMMENT 'resolved through lowercase'",
    )

    [column] = read_live_table(live_connection, table_name)["columns"]
    assert column["column_name"] == "requestId"
    assert column["comment"] == "resolved through lowercase"


def test_column_identifier_case_repro_metadata_sync_matches_contract_schema(
    live_connection, live_tables
):
    """Metadata sync accepts camelCase display names without structural drift."""
    table_name = live_tables("column_case_metadata_sync")
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

    report = engine.sync(declaration)

    assert report.has_failures is False
    assert report.has_changes is True
    statements = next(iter(report.planned_sql_statements.values()))
    assert any("ALTER COLUMN `requestid` COMMENT" in statement for statement in statements)
    assert any("ALTER COLUMN `modelid` COMMENT" in statement for statement in statements)

    state = read_live_table(live_connection, table_name)
    columns = {column["column_name"]: column for column in state["columns"]}
    assert columns["requestId"]["comment"] == "AWS request identifier"
    assert columns["modelId"]["comment"] == "Full Bedrock model identifier"
    assert state["comment"] == "Bedrock request invocations"
    assert engine.sync(declaration).has_changes is False
