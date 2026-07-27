"""
Live pins for camelCase column identifier handling.

The platform facts: Unity Catalog preserves a column's display spelling,
ordinary ALTER COLUMN resolves lowercase references against camelCase
columns, but the managed-constraint path does not. The engine therefore
binds constraint references to the catalog's exact spelling; these tests
pin both the platform behaviour and the engine's convergence.
"""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine
from delta_engine.schema import Column, DeltaTable, ForeignKey, Integer, String, Timestamp
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


def test_column_identifier_case_metadata_sync_adds_primary_key_with_exact_spelling(
    live_connection, live_tables
):
    """Metadata sync adds a primary key using the catalog's camelCase spelling."""
    table_name = live_tables("column_case_add_primary_key")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (`requestId` STRING NOT NULL) USING DELTA",
    )
    declaration = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=table_name,
        columns=(Column("requestid", String(), nullable=False),),
        primary_key=("requestid",),
        scope="metadata",
    )
    engine = build_sql_engine(live_connection)

    report = engine.sync(declaration)

    assert report.has_failures is False
    statements = next(iter(report.planned_sql_statements.values()))
    assert statements == (
        f"ALTER TABLE {qualified_table(table_name)} "
        f"ADD CONSTRAINT `{table_name}_pk` PRIMARY KEY (`requestId`)",
    )
    state = read_live_table(live_connection, table_name)
    assert state["primary_key"] == ("requestId",)
    assert state["primary_key_name"] == f"{table_name}_pk"
    assert engine.sync(declaration).has_changes is False


def test_column_identifier_case_repro_metadata_sync_matches_contract_schema(
    live_connection, live_tables
):
    """Metadata sync accepts camelCase display names without structural drift."""
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


def test_column_identifier_case_repro_real_name_mismatch_reports_structural_drift(
    live_connection, live_tables
):
    """Metadata sync rejects request_id as structurally different from requestId."""
    table_name = live_tables("column_case_real_mismatch")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (`request_id` STRING) USING DELTA",
    )
    declaration = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=table_name,
        columns=(Column("requestId", String()),),
        scope="metadata",
    )

    report = build_sql_engine(live_connection).sync(declaration, dry_run=True)

    [table_report] = tuple(report)
    assert table_report.has_changes is False
    assert table_report.planned_sql_statements == ()
    [failure] = table_report.failures
    assert failure.rule_name == "UnmanagedAspectDrift"
    assert "column structure" in failure.message


def test_column_identifier_case_foreign_key_binds_exact_spelling_on_both_sides(
    live_connection, live_tables
):
    """A foreign key compiles with each table's exact catalog spelling and converges."""
    parent_name = live_tables("column_case_fk_parent")
    child_name = live_tables("column_case_fk_child")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(parent_name)} (`OrderId` STRING NOT NULL) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(parent_name)} "
        f"ADD CONSTRAINT `{parent_name}_pk` PRIMARY KEY (`OrderId`)",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} (`orderRef` STRING) USING DELTA",
    )

    parent = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=parent_name,
        columns=(Column("orderid", String(), nullable=False),),
        primary_key=("orderid",),
        scope="metadata",
    )
    child = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=child_name,
        columns=(Column("orderref", String()),),
        foreign_keys=(ForeignKey(columns={"orderref": "orderid"}, references=parent),),
        scope="metadata",
    )
    engine = build_sql_engine(live_connection)

    report = engine.sync(parent, child)

    assert report.has_failures is False
    child_statements = report.planned_sql_statements[
        f"{live_catalog()}.{live_schema()}.{child_name}"
    ]
    assert any(
        "FOREIGN KEY (`orderRef`)" in statement and "(`OrderId`)" in statement
        for statement in child_statements
    )
    assert engine.sync(parent, child).has_changes is False
