"""
Live pins for Databricks platform rules the engine's declaration gates assume.

Each test states a platform behaviour the engine relies on but cannot verify
locally: if a runtime upgrade changes one, the corresponding gate is either
over-blocking or under-blocking and needs revisiting.
"""

import pytest

pytest.importorskip("databricks.sql")

from databricks.sql.exc import ServerOperationError

from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    qualified_table,
    read_live_table,
)


def test_cdf_enablement_fails_on_a_table_carrying_reserved_cdf_columns(
    live_connection, live_tables
):
    # The API refuses declarations that combine change data feed with
    # _change_type/_commit_version/_commit_timestamp columns because the
    # platform enforces the same rule at enablement time.
    table_name = live_tables("cdf_reserved")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (id INT, _change_type STRING) USING DELTA",
    )

    with pytest.raises(ServerOperationError, match="DELTA_TABLE_ALREADY_CONTAINS_CDC_COLUMNS"):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} "
            "SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')",
        )


def test_special_characters_in_nested_struct_field_names_require_column_mapping(
    live_connection, live_tables
):
    # The declaration gate recursively rejects special characters in nested
    # struct field names unless column mapping is on — both directions of
    # that assumption must match the platform. (The engine deliberately does
    # not round-trip such tables yet: the reader cannot parse the unquoted
    # DDL rendering `struct<bad name:int>`; see the todo on special-character
    # field names.)
    rejected_name = live_tables("nested_plain")
    with pytest.raises(ServerOperationError, match="DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES"):
        execute_sql(
            live_connection,
            f"CREATE TABLE {qualified_table(rejected_name)} "
            "(s STRUCT<`bad name`: INT>) USING DELTA",
        )

    accepted_name = live_tables("nested_mapped")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(accepted_name)} "
        "(s STRUCT<`bad name`: INT>) USING DELTA "
        "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
    )
    state = read_live_table(live_connection, accepted_name)
    assert "bad name" in state["columns"][0]["full_data_type"]


def test_platform_refuses_clustering_a_partitioned_table(live_connection, live_tables):
    # PartitioningChangeNotSupported blocks partitioned->clustered
    # conversions; the platform refuses the direct conversion too, so the
    # engine is not withholding a supported operation.
    table_name = live_tables("part_to_cluster")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} "
        "(id INT, region STRING) USING DELTA PARTITIONED BY (region)",
    )

    with pytest.raises(
        ServerOperationError,
        match="CLUSTER_BY_ON_PARTITIONED_TABLE",
    ):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} CLUSTER BY (`id`)",
        )
