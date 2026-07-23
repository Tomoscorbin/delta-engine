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


def test_sync_creates_and_round_trips_every_supported_column_type(live_connection, live_tables):
    """Every supported column type, including nested and parameterised ones, round-trips."""
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

    engine = build_sql_engine(live_connection)
    engine.sync(table)

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
    # Creation is only half the round trip: the reader must parse every nested
    # and parameterised type — array<int>, map<string,bigint>, the nested
    # struct, decimal(18,4), variant — back into a domain type equal to the
    # declaration, or the table would re-diff forever. A converged resync
    # proves the whole type surface round-trips through the engine's reader.
    assert engine.sync(table).has_changes is False


def test_sync_creates_every_managed_table_property(live_connection, live_tables):
    """Every managed table property is written and read back on creation."""
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
    """A freshly created table carries none of the managed property keys."""
    # Property-policy admission: a key belongs in the managed set only
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


def test_unmanaged_properties_are_invisible_to_a_full_sync(live_connection, live_tables):
    """Unmanaged properties are neither drift nor unset on a full sync."""
    # The reader filters unmanaged keys out of observed state, so keys
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
    """Partition, clustering, and primary-key columns can all be widened."""
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
    """A partitioned table is created with its partition columns in declared order."""
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
    """Liquid clustering keys can be changed and then removed entirely."""
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
    """Supported numeric, decimal, and temporal column types widen in place."""
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
                Column("measure", Integer()),
                Column("ratio", Float()),
                Column("event_date", Date()),
                Column("amount", Decimal(10, 2)),
                Column("identifier", Long()),
            ),
        )
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
                Column("measure", Double()),
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
        "measure": "double",
        "ratio": "double",
        "event_date": "timestamp_ntz",
        "amount": "decimal(12,3)",
        "identifier": "decimal(22,2)",
    }


def test_create_with_timestamp_ntz_enables_feature_and_resyncs_clean(
    live_connection, live_tables
):
    """
    Create-time feature enablement is visible to observation: the resync plans nothing.

    This is the pin carrying the AS JSON observation choice: if it fails, the
    platform does not surface create-time-enabled features as delta.feature.*
    properties, and the reader must switch to DESCRIBE DETAIL.tableFeatures
    (see the 2026-07-23 table-feature design spec).
    """
    table_name = live_tables("ntz_create")
    engine = build_sql_engine(live_connection)
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer()), Column("seen_at", TimestampNtz())),
    )
    engine.sync(table)
    assert "timestampNtz" in read_live_table(live_connection, table_name)["features"]

    report = engine.sync(table)

    [table_report] = report.table_reports
    assert not table_report.planned_sql_statements


def test_adding_timestamp_ntz_column_plans_feature_enable_before_add(
    live_connection, live_tables
):
    """Adding TIMESTAMP_NTZ to an existing table converges without out-of-band steps."""
    table_name = live_tables("ntz_add")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
        )
    )
    assert "timestampNtz" not in read_live_table(live_connection, table_name)["features"]

    extended = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer()), Column("seen_at", TimestampNtz())),
    )
    report = engine.sync(extended)

    [table_report] = report.table_reports
    statements = table_report.planned_sql_statements
    enable_index = next(
        index
        for index, statement in enumerate(statements)
        if "delta.feature.timestampNtz" in statement
    )
    add_index = next(
        index for index, statement in enumerate(statements) if "ADD COLUMN" in statement
    )
    assert enable_index < add_index
    assert "timestampNtz" in read_live_table(live_connection, table_name)["features"]
    assert not engine.sync(extended).table_reports[0].planned_sql_statements


def test_variant_feature_enablement_round_trips(live_connection, live_tables):
    """
    VARIANT: create-time enablement resyncs clean; add-column plans the enable.

    Also resolves the enable-key question: if the SET TBLPROPERTIES with
    'delta.feature.variantType-preview' is rejected here, flip _ENABLE_NAMES
    in adapters/databricks/sql/features.py to the GA name 'variantType'
    (observation already accepts both names).
    """
    created = live_tables("variant_create")
    engine = build_sql_engine(live_connection)
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        created,
        columns=(Column("id", Integer()), Column("payload", Variant())),
    )
    engine.sync(table)
    features = read_live_table(live_connection, created)["features"]
    assert {"variantType", "variantType-preview"} & set(features)
    assert not engine.sync(table).table_reports[0].planned_sql_statements

    extended_name = live_tables("variant_add")
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            extended_name,
            columns=(Column("id", Integer()),),
        )
    )
    extended = DeltaTable(
        live_catalog(),
        live_schema(),
        extended_name,
        columns=(Column("id", Integer()), Column("payload", Variant())),
    )
    report = engine.sync(extended)
    [table_report] = report.table_reports
    assert any(
        "delta.feature.variantType" in statement
        for statement in table_report.planned_sql_statements
    )
    assert not engine.sync(extended).table_reports[0].planned_sql_statements
