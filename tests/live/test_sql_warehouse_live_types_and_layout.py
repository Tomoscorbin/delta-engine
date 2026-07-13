"""Live coverage for the public type, property, partitioning, and clustering surface."""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine
from delta_engine.schema import (
    Array,
    Binary,
    Boolean,
    Byte,
    Column,
    Date,
    Decimal,
    DeltaTable,
    Double,
    Float,
    Integer,
    Long,
    Map,
    Property,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
    Variant,
)
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
    read_live_table,
)


def _types(state) -> dict[str, str]:
    return {
        column["column_name"]: column["full_data_type"].casefold().replace(" ", "")
        for column in state["columns"]
    }


def test_sync_creates_every_supported_column_type(live_connection, live_tables):
    table_name = live_tables("types")
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("tiny", Byte()),
            Column("small", Short()),
            Column("integer_value", Integer()),
            Column("long_value", Long()),
            Column("float_value", Float()),
            Column("double_value", Double()),
            Column("decimal_value", Decimal(18, 4)),
            Column("boolean_value", Boolean()),
            Column("string_value", String()),
            Column("binary_value", Binary()),
            Column("date_value", Date()),
            Column("timestamp_value", Timestamp()),
            Column("timestamp_ntz_value", TimestampNtz()),
            Column("variant_value", Variant()),
            Column("array_value", Array(Integer())),
            Column("map_value", Map(String(), Long())),
            Column(
                "struct_value",
                Struct(
                    (
                        StructField("name", String()),
                        StructField(
                            "location",
                            Struct(
                                (
                                    StructField("latitude", Double()),
                                    StructField("longitude", Double()),
                                )
                            ),
                        ),
                    )
                ),
            ),
        ),
    )

    build_sql_engine(live_connection).sync(table)

    assert _types(read_live_table(live_connection, table_name)) == {
        "tiny": "tinyint",
        "small": "smallint",
        "integer_value": "int",
        "long_value": "bigint",
        "float_value": "float",
        "double_value": "double",
        "decimal_value": "decimal(18,4)",
        "boolean_value": "boolean",
        "string_value": "string",
        "binary_value": "binary",
        "date_value": "date",
        "timestamp_value": "timestamp",
        "timestamp_ntz_value": "timestamp_ntz",
        "variant_value": "variant",
        "array_value": "array<int>",
        "map_value": "map<string,bigint>",
        "struct_value": "struct<name:string,location:struct<latitude:double,longitude:double>>",
    }


def test_sync_creates_every_managed_table_property(live_connection, live_tables):
    table_name = live_tables("properties")
    declared = {
        Property.COLUMN_MAPPING_MODE: "name",
        Property.CHANGE_DATA_FEED: "true",
        Property.DELETED_FILE_RETENTION_DURATION: "interval 7 days",
        Property.LOG_RETENTION_DURATION: "interval 30 days",
        Property.DATA_SKIPPING_NUM_INDEXED_COLS: "-1",
        Property.TYPE_WIDENING: "false",
    }

    build_sql_engine(live_connection).sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
            properties=declared,
        )
    )

    properties = read_live_table(live_connection, table_name)["properties"]
    assert {property_name: properties[property_name] for property_name in declared} == declared


def test_fresh_table_carries_no_managed_property_keys(live_connection, live_tables):
    # Registry admission policy: a key belongs in the managed registry only
    # if Databricks does not auto-write it, otherwise every undeclared table
    # would fail validation on resync.
    table_name = live_tables("fresh_properties")
    build_sql_engine(live_connection).sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
        )
    )

    properties = read_live_table(live_connection, table_name)["properties"]
    assert not set(properties) & {str(key) for key in Property}


def test_unregistered_properties_are_invisible_to_a_full_sync(live_connection, live_tables):
    # The reader filters unregistered keys out of observed state, so keys
    # owned by other tooling or the platform are neither drift nor unset.
    table_name = live_tables("custom_properties")
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer()),),
    )
    engine = build_sql_engine(live_connection)
    engine.sync(declaration)
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(table_name)} SET TBLPROPERTIES ('team.owner'='governance')",
    )

    report = engine.sync(declaration)

    assert report.has_failures is False
    assert report.has_changes is False
    assert read_live_table(live_connection, table_name)["properties"]["team.owner"] == "governance"


def test_sync_widens_partition_clustering_and_key_columns(live_connection, live_tables):
    # The engine does not model platform restrictions on which column roles
    # may widen; Unity Catalog imposes none for these roles, so the plain
    # widening path must succeed on partition, clustering, and key columns.
    partitioned_name = live_tables("widen_partition_role")
    clustered_name = live_tables("widen_cluster_role")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            partitioned_name,
            columns=(Column("id", Short(), nullable=False), Column("bucket", Short())),
            partitioned_by=("bucket",),
            primary_key=("id",),
            properties={Property.TYPE_WIDENING: "true"},
        ),
        DeltaTable(
            live_catalog(),
            live_schema(),
            clustered_name,
            columns=(Column("id", Short()), Column("payload", String())),
            clustered_by=("id",),
            properties={Property.TYPE_WIDENING: "true"},
        ),
    )

    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            partitioned_name,
            columns=(Column("id", Integer(), nullable=False), Column("bucket", Integer())),
            partitioned_by=("bucket",),
            primary_key=("id",),
            properties={Property.TYPE_WIDENING: "true"},
        ),
        DeltaTable(
            live_catalog(),
            live_schema(),
            clustered_name,
            columns=(Column("id", Integer()), Column("payload", String())),
            clustered_by=("id",),
            properties={Property.TYPE_WIDENING: "true"},
        ),
    )

    assert _types(read_live_table(live_connection, partitioned_name)) == {
        "id": "int",
        "bucket": "int",
    }
    assert _types(read_live_table(live_connection, clustered_name)) == {
        "id": "int",
        "payload": "string",
    }


def test_sync_creates_partitioned_table_with_ordered_partition_columns(
    live_connection, live_tables
):
    table_name = live_tables("partitioned")
    build_sql_engine(live_connection).sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("id", Integer()),
                Column("event_date", Date()),
                Column("region", String()),
            ),
            partitioned_by=("event_date", "region"),
        )
    )

    state = read_live_table(live_connection, table_name)
    assert state["partitioning"] == ("event_date", "region")
    assert {column["column_name"]: column["partition_index"] for column in state["columns"]} == {
        "id": None,
        "event_date": 0,
        "region": 1,
    }


def test_sync_changes_and_removes_liquid_clustering(live_connection, live_tables):
    table_name = live_tables("clustering")
    columns = (Column("id", Integer()), Column("region", String()))
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=columns,
            clustered_by=("region",),
        )
    )
    assert read_live_table(live_connection, table_name)["clustering"] == ("region",)

    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=columns,
            clustered_by=("id", "region"),
        )
    )
    assert set(read_live_table(live_connection, table_name)["clustering"]) == {"id", "region"}

    engine.sync(
        DeltaTable(live_catalog(), live_schema(), table_name, columns=columns, clustered_by=())
    )
    assert read_live_table(live_connection, table_name)["clustering"] == ()


def test_sync_widens_supported_column_types_in_live_catalog(live_connection, live_tables):
    table_name = live_tables("widen")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("tiny", Byte()),
                Column("small", Short()),
                Column("count", Integer()),
                Column("ratio", Float()),
                Column("event_date", Date()),
                Column("amount", Decimal(10, 2)),
                Column("identifier", Long()),
            ),
        )
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(table_name)} "
        "SET TBLPROPERTIES ('delta.feature.timestampNtz'='supported')",
    )

    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("tiny", Short()),
                Column("small", Integer()),
                Column("count", Long()),
                Column("ratio", Double()),
                Column("event_date", TimestampNtz()),
                Column("amount", Decimal(12, 3)),
                Column("identifier", Decimal(22, 2)),
            ),
            properties={Property.TYPE_WIDENING: "true"},
        )
    )

    assert _types(read_live_table(live_connection, table_name)) == {
        "tiny": "smallint",
        "small": "int",
        "count": "bigint",
        "ratio": "double",
        "event_date": "timestamp_ntz",
        "amount": "decimal(12,3)",
        "identifier": "decimal(22,2)",
    }
