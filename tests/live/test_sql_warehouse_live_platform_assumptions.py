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


def test_platform_rename_silently_drops_keys_including_other_tables_foreign_keys(
    live_connection, live_tables
):
    # RENAME COLUMN drops any PK/FK using the column, cascading into other
    # tables' foreign keys with no error (first observed live 2026-07-14).
    # This is the hazard behind PrimaryKeyReferencedByForeignKeys and the
    # reason plans state key drops explicitly instead of relying on the
    # rename: if this ever stops cascading, those become merely redundant.
    parent_name = live_tables("cascade_parent")
    child_name = live_tables("cascade_child")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(parent_name)} (id INT NOT NULL, "
        f"CONSTRAINT {parent_name}_pk PRIMARY KEY (id)) USING DELTA "
        "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} (parent_id INT, "
        f"CONSTRAINT {child_name}_fk FOREIGN KEY (parent_id) "
        f"REFERENCES {qualified_table(parent_name)} (id)) USING DELTA",
    )

    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(parent_name)} RENAME COLUMN id TO account_id",
    )

    assert read_live_table(live_connection, parent_name)["primary_key"] == ()
    assert read_live_table(live_connection, child_name)["foreign_keys"] == ()


def test_platform_restricts_a_primary_key_drop_while_a_foreign_key_references_it(
    live_connection, live_tables
):
    # DROP PRIMARY KEY defaults to RESTRICT, and IF EXISTS does not bypass
    # it. This is the engine's fail-closed net: an inbound FK the reader
    # could not observe (e.g. cross-catalog) fails the compiled drop, and
    # execution stops before the rename can cascade.
    parent_name = live_tables("restrict_parent")
    child_name = live_tables("restrict_child")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(parent_name)} (id INT NOT NULL, "
        f"CONSTRAINT {parent_name}_pk PRIMARY KEY (id)) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} (parent_id INT, "
        f"CONSTRAINT {child_name}_fk FOREIGN KEY (parent_id) "
        f"REFERENCES {qualified_table(parent_name)} (id)) USING DELTA",
    )

    with pytest.raises(ServerOperationError, match="child constraints"):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(parent_name)} DROP PRIMARY KEY IF EXISTS",
        )

    assert read_live_table(live_connection, parent_name)["primary_key"] == ("id",)


def test_platform_blocks_renaming_a_column_referenced_by_a_check_constraint(
    live_connection, live_tables
):
    # The engine does not model CHECK constraints; renames of referenced
    # columns are documented to fail at execution rather than validation.
    table_name = live_tables("check_dependent")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (amount INT) USING DELTA "
        "TBLPROPERTIES ('delta.columnMapping.mode'='name')",
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(table_name)} "
        f"ADD CONSTRAINT {table_name}_positive CHECK (amount > 0)",
    )

    with pytest.raises(ServerOperationError, match="DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE"):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} RENAME COLUMN amount TO amt",
        )


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


def test_platform_rejects_an_over_long_column_tag_key_or_value(live_connection, live_tables):
    # A column tag key or value longer than 256 characters is rejected (first
    # observed live 2026-07-14). This backs the length gates in
    # api/delta_table.py (_validate_tags), which reject both at declaration
    # time; before it was confirmed the value gate allowed 1000 characters and
    # there was no key gate at all.
    table_name = live_tables("tag_length")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (id INT) USING DELTA",
    )
    over_long = "x" * 300

    # A 300-character tag value (short key).
    with pytest.raises(ServerOperationError, match=r"(?i)length|character|exceed"):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} ALTER COLUMN id "
            f"SET TAGS ('team'='{over_long}')",
        )

    # A 300-character tag key (short value).
    with pytest.raises(ServerOperationError, match=r"(?i)length|character|exceed"):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} ALTER COLUMN id "
            f"SET TAGS ('{over_long}'='prod')",
        )


def test_platform_rejects_complex_types_as_partition_columns(live_connection, live_tables):
    # Delta refuses complex/nested types as partition columns. This backs the
    # layout gate in api/delta_table.py (_TYPES_UNUSABLE_AS_LAYOUT_KEYS), which
    # rejects Array/Map/Struct/Variant as partition and clustering keys at
    # declaration time; if the platform ever accepted one, that gate would be
    # over-blocking. The parallel clustering rejection is a distinct backend
    # rule whose error code is not pinned here.
    array_name = live_tables("partition_array")
    with pytest.raises(ServerOperationError, match="DELTA_INVALID_PARTITION_COLUMN_TYPE"):
        execute_sql(
            live_connection,
            f"CREATE TABLE {qualified_table(array_name)} "
            "(id INT, labels ARRAY<INT>) USING DELTA PARTITIONED BY (labels)",
        )

    struct_name = live_tables("partition_struct")
    with pytest.raises(ServerOperationError, match="DELTA_INVALID_PARTITION_COLUMN_TYPE"):
        execute_sql(
            live_connection,
            f"CREATE TABLE {qualified_table(struct_name)} "
            "(id INT, payload STRUCT<x: INT>) USING DELTA PARTITIONED BY (payload)",
        )
