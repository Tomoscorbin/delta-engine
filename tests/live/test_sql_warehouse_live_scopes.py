"""
Live proofs that restricted scopes change only the catalog aspects they own.

There is deliberately no ``annotations`` pin here. Every statement form that
scope emits against an ordinary Delta table is already driven by the metadata
and tags pins below, so a third would state no platform fact they do not. What
distinguishes it — reaching a streaming table, where keys and structure belong
to the pipeline — is pinned in ``test_sql_warehouse_live_streaming_tables.py``.
"""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine
from delta_engine.schema import Column, DeltaTable, Integer, String
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
    read_live_table,
)


def test_metadata_scope_changes_metadata_and_preserves_external_ddl(live_connection, live_tables):
    """A metadata-scoped sync updates governed metadata but preserves external structure."""
    # Given an externally created table with its own comments, properties,
    # and clustering
    table_name = live_tables("metadata_scope")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} "
        "(id INT NOT NULL COMMENT 'external id', payload STRING) USING DELTA "
        "COMMENT 'external comment' "
        "TBLPROPERTIES ('delta.logRetentionDuration'='interval 10 days') "
        "CLUSTER BY (id)",
    )

    # When syncing a metadata-scoped declaration with governed comments,
    # tags, and a key
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

    # Then the governed metadata changed while the external structure and
    # properties survived
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
    # Given an externally created table carrying a tag to remove
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

    # When syncing a tags-scoped declaration with new table and column tags
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

    # Then only the tags changed
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
