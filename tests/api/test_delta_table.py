from types import MappingProxyType

import pytest

from delta_engine.application.scopes import METADATA_ASPECTS, TAG_ASPECTS
from delta_engine.domain.model import (
    ALL_ASPECTS,
    DesiredColumn as DomainColumn,
    QualifiedName,
    TableAspect,
)
from delta_engine.domain.model.constraints import PrimaryKeyConstraint
from delta_engine.schema import (
    Array,
    Binary,
    Boolean,
    Column,
    Date,
    DeltaTable,
    ForeignKey,
    Integer,
    Long,
    Map,
    Property,
    Self,
    String,
    Struct,
    StructField,
    TimestampNtz,
    Variant,
)

# ---------- introspection accessors ----------


def test_delta_table_exposes_declared_name_parts():
    # Given a table declared with distinct catalog, schema, and name parts
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
    )

    # Then each name part is readable back as its own string
    assert table.catalog == "cat"
    assert table.schema == "sales"
    assert table.name == "orders"


def test_mixed_case_declaration_normalizes_to_lowercase():
    # Identifiers are case-insensitive on Databricks and Unity Catalog stores
    # object names lowercase, so the declaration canonicalizes rather than rejects
    table = DeltaTable(
        catalog="Main",
        schema="Sales",
        name="Orders",
        columns=[Column("Id", Integer())],
    )

    assert (table.catalog, table.schema, table.name) == ("main", "sales", "orders")
    assert [column.name for column in table.columns] == ["id"]


@pytest.mark.parametrize(
    "bad_part",
    ["or.ders", "or ders", "or/ders", "or\x07ders", "or\x7fders", "x" * 256],
    ids=["period", "space", "slash", "control-char", "del", "over-255-chars"],
)
@pytest.mark.parametrize("position", ["catalog", "schema", "name"])
def test_rejects_name_parts_unity_catalog_forbids(position, bad_part):
    # Unity Catalog object names: at most 255 characters; no period, space,
    # forward slash, ASCII control characters, or DEL
    parts = {"catalog": "main", "schema": "sales", "name": "orders", position: bad_part}
    with pytest.raises(ValueError, match="Unity Catalog"):
        DeltaTable(
            catalog=parts["catalog"],
            schema=parts["schema"],
            name=parts["name"],
            columns=[Column("id", Integer())],
        )


def test_column_names_are_exempt_from_object_name_rules():
    # Column-name characters are governed by column mapping, not the Unity
    # Catalog object-name rules: a forward slash needs no special handling
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer()), Column("net/gross", String())],
    )

    assert [column.name for column in table.columns] == ["id", "net/gross"]


def test_delta_table_exposes_declared_columns():
    # Given a table declared with ordered columns
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer(), nullable=False), Column("name", String())],
    )

    # Then columns reads back the declared columns in order
    assert tuple(column.name for column in table.columns) == ("id", "name")
    assert all(isinstance(column, DomainColumn) for column in table.columns)


def test_delta_table_exposes_declared_comment():
    # Given a table declared with a comment
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
        comment="Daily orders",
    )

    # Then comment reads it back
    assert table.comment == "Daily orders"


def test_delta_table_comment_defaults_to_empty_string():
    # Given a table declared without a comment
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
    )

    # Then comment is an empty string, never None
    assert table.comment == ""


def test_delta_table_exposes_declared_properties_including_absence_assertions():
    # Given a table declaring one property value and one absence assertion
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
        properties={
            Property.CHANGE_DATA_FEED.value: "true",
            Property.COLUMN_MAPPING_MODE.value: None,
        },
    )

    # Then properties reads them back, preserving the None absence assertion
    assert dict(table.properties) == {
        Property.CHANGE_DATA_FEED.value: "true",
        Property.COLUMN_MAPPING_MODE.value: None,
    }


def test_delta_table_exposes_declared_tags():
    # Given a table declared with tags
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
        tags={"env": "prod"},
    )

    # Then tags reads them back
    assert dict(table.tags) == {"env": "prod"}


def test_delta_table_exposes_declared_partitioning():
    # Given a table partitioned by a column
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer()), Column("ds", String())],
        partitioned_by=["ds"],
    )

    # Then partitioned_by reads it back
    assert table.partitioned_by == ("ds",)


def test_delta_table_exposes_declared_clustering():
    # Given a table declared with clustering keys
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[
            Column("id", Integer()),
            Column("region", String()),
            Column("day", Date()),
        ],
        clustered_by=["region", "day"],
    )
    # Then clustered_by reads them back in declaration order
    assert table.clustered_by == ("region", "day")


def test_delta_table_clustering_defaults_to_empty_tuple():
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
    )
    assert table.clustered_by == ()


def test_delta_table_rejects_partitioning_and_clustering_together():
    with pytest.raises(ValueError, match="both partition"):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column("id", Integer()), Column("region", String())],
            partitioned_by=["id"],
            clustered_by=["region"],
        )


def test_delta_table_rejects_more_than_four_clustering_keys():
    with pytest.raises(ValueError, match="four"):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column(name, Integer()) for name in ("a", "b", "c", "d", "e")],
            clustered_by=["a", "b", "c", "d", "e"],
        )


def test_delta_table_rejects_complex_typed_clustering_column():
    with pytest.raises(ValueError, match="clustering key"):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column("id", Integer()), Column("attrs", Map(String(), String()))],
            clustered_by=["attrs"],
        )


@pytest.mark.parametrize(
    ("layout", "message"),
    [
        ({"partitioned_by": ["ITEMS"]}, "Delta cannot partition"),
        ({"clustered_by": ["ITEMS"]}, "clustering key"),
    ],
)
def test_layout_keys_are_normalized_before_type_validation(layout, message):
    # Column declarations are already canonical lowercase. Layout identifiers
    # must use the same representation before API policy tries to resolve them.
    with pytest.raises(ValueError, match=message):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column("ID", Integer()), Column("Items", Array(String()))],
            **layout,
        )


@pytest.mark.parametrize("data_type", [Boolean(), Binary()])
def test_delta_table_rejects_boolean_or_binary_clustering_column(data_type):
    # Liquid clustering's supported-type list excludes Boolean and Binary, even
    # though both are valid partition columns.
    with pytest.raises(ValueError, match="clustering key"):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column("id", Integer()), Column("flag", data_type)],
            clustered_by=["flag"],
        )


@pytest.mark.parametrize("data_type", [Boolean(), Binary()])
def test_delta_table_accepts_boolean_or_binary_partition_column(data_type):
    # Partitioning and clustering have distinct type rules: Boolean and Binary
    # are rejected as clustering keys but accepted as partition columns.
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer()), Column("flag", data_type)],
        partitioned_by=["flag"],
    )

    assert table.partitioned_by == ("flag",)


@pytest.mark.parametrize(
    "bad_keys",
    [
        ["delta.random_thing"],
        ["foo", "bar.baz"],  # multiple, order should not matter in message
    ],
)
def test_rejects_unknown_table_property_keys(bad_keys):
    # Given user supplied properties that are not recognised by the Property enum
    user_properties = {k: "x" for k in bad_keys}

    # When/then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="coredev",
            schema="medallia",
            name="responses",
            columns=[Column("id", Integer())],
            properties=user_properties,
        )


def test_accepts_only_enum_property_keys():
    # Given user supplied allowed keys from the enum
    user_properties = {
        Property.CHANGE_DATA_FEED.value: "true",
        Property.COLUMN_MAPPING_MODE.value: "name",
    }

    # When constructing the table
    table = DeltaTable(
        catalog="coredev",
        schema="medallia",
        name="responses",
        columns=[Column("id", Integer())],
        properties=user_properties,
    )

    # Then it succeeds and the keys are intact
    properties = table.to_desired_table().properties
    assert properties[Property.CHANGE_DATA_FEED.value] == "true"
    assert properties[Property.COLUMN_MAPPING_MODE.value] == "name"


def test_accepts_property_enum_members_as_keys():
    # Given properties keyed by the Property enum members directly (not their .value)
    user_properties = {Property.CHANGE_DATA_FEED: "true"}

    # When constructing the table
    table = DeltaTable(
        catalog="coredev",
        schema="medallia",
        name="responses",
        columns=[Column("id", Integer())],
        properties=user_properties,
    )

    # Then the enum key is accepted and resolves to the same managed property as
    # its string value, so callers can declare properties without reaching for .value
    desired = table.to_desired_table()
    assert desired.properties[Property.CHANGE_DATA_FEED.value] == "true"


def test_partition_columns_must_exist():
    # Given columns include 'event_date' and the partition spec references it
    table = DeltaTable(
        catalog="coredev",
        schema="medallia",
        name="responses",
        columns=[Column("id", Integer()), Column("event_date", String())],
        partitioned_by=["event_date"],
    )

    # When converting to the domain table
    desired = table.to_desired_table()

    # Then conversion succeeds and partitioning is preserved
    assert desired.partitioned_by == ("event_date",)


def test_missing_partition_column_raises_error():
    # Given a partition spec referencing a column that does not exist
    # Then construction itself fails — invalid definitions are rejected immediately
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="coredev",
            schema="medallia",
            name="responses",
            columns=[Column("id", Integer()), Column("event_date", String())],
            partitioned_by=["store_id"],  # not present
        )


def test_delta_table_rejects_complex_typed_partition_column() -> None:
    with pytest.raises(ValueError, match="Delta cannot partition"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[
                Column("id", Integer()),
                Column("items", Array(String())),
            ],
            partitioned_by=["items"],
        )


def test_delta_table_rejects_partitioning_by_every_column() -> None:
    with pytest.raises(ValueError, match="every column"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer()), Column("day", String())],
            partitioned_by=["id", "day"],
        )


def test_partition_keys_are_normalized_before_whole_table_validation() -> None:
    with pytest.raises(ValueError, match="every column"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("ID", Integer()), Column("Day", String())],
            partitioned_by=["ID", "DAY"],
        )


def test_to_desired_table_preserves_columns_and_metadata():
    # Given a table with explicit column metadata, comment, and partitioning
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="fact_orders",
        columns=[
            Column("id", Integer(), nullable=False, comment="primary key"),
            Column("ds", String(), comment="partition date"),
        ],
        comment="Daily aggregated orders",
        partitioned_by=["ds"],
    )

    # When converting to the domain table
    desired = table.to_desired_table()

    # Then the qualified name, columns, comment, and partitioning carry through
    assert str(desired.qualified_name) == "cat.sales.fact_orders"
    assert all(isinstance(c, DomainColumn) for c in desired.columns)
    assert [c.name for c in desired.columns] == ["id", "ds"]
    assert [c.nullable for c in desired.columns] == [False, True]
    assert [c.comment for c in desired.columns] == ["primary key", "partition date"]
    assert desired.comment == "Daily aggregated orders"
    assert desired.partitioned_by == ("ds",)


def test_to_desired_table_defaults_partitioning_to_empty_tuple():
    # Given a table with no partition specs
    table = DeltaTable(
        catalog="cat",
        schema="core",
        name="dim_date",
        columns=[Column("id", Integer())],
    )

    # When converting to the domain table
    desired = table.to_desired_table()

    # Then partitioned_by is a stable empty tuple, never None
    assert desired.partitioned_by == ()


def test_to_desired_table_derives_the_table_features_its_types_require():
    # Given a declaration whose types need Delta table features, nested and not
    table = DeltaTable(
        catalog="cat",
        schema="core",
        name="events",
        columns=[
            Column("id", Integer()),
            Column("seen_at", TimestampNtz()),
            Column("payload", Map(String(), Variant())),
        ],
    )

    # When converting to the domain table
    desired = table.to_desired_table()

    # Then the required feature names travel with the declaration, so nothing
    # downstream has to inspect types to plan their enablement
    assert desired.required_features == frozenset({"timestampNtz", "variantType"})


def test_to_desired_table_requires_no_features_for_ordinary_types():
    table = DeltaTable(
        catalog="cat",
        schema="core",
        name="dim_date",
        columns=[Column("id", Integer()), Column("label", String())],
    )

    assert table.to_desired_table().required_features == frozenset()


def test_primary_key_parameter_lowers_into_table_level_constraint():
    # Given a composite key declared at table level
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )

    # Then the constraint carries the declared columns and generated name
    desired = table.to_desired_table()
    assert desired.primary_key is not None
    assert desired.primary_key.columns == ("tenant_id", "id")
    assert desired.primary_key.constraint_name == "accounts_pk"


def test_no_primary_key_parameter_means_no_key():
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="events",
        columns=[Column("id", Integer())],
    )

    assert table.to_desired_table().primary_key is None
    assert table.primary_key == ()


def test_empty_primary_key_sequence_is_rejected():
    # Given an empty sequence — a meaningless assertion, distinct from None
    with pytest.raises(ValueError, match="must not be empty"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="events",
            columns=[Column("id", Integer())],
            primary_key=[],
        )


def test_primary_key_as_a_bare_string_is_rejected():
    # "i" is a real column, so without the guard this string would silently
    # declare a valid per-character key; the shape must be refused outright
    with pytest.raises(TypeError, match="not a string"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="events",
            columns=[Column("i", Integer(), nullable=False)],
            primary_key="i",
        )


def test_primary_key_naming_unknown_column_is_rejected():
    with pytest.raises(ValueError, match="missing_col"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="events",
            columns=[Column("id", Integer(), nullable=False)],
            primary_key=["missing_col"],
        )


def test_primary_key_over_nullable_column_is_rejected():
    with pytest.raises(ValueError, match="NOT NULL"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="events",
            columns=[Column("id", Integer(), nullable=True)],
            primary_key=["id"],
        )


def test_column_no_longer_accepts_a_primary_key_flag():
    # The per-column flag is deleted; the fact lives at table level now
    with pytest.raises(TypeError):
        Column("id", Integer(), nullable=False, primary_key=True)  # type: ignore[call-arg]


def test_delta_table_primary_key_returns_pk_column_names():
    # Given a DeltaTable with one PK column
    table = DeltaTable(
        catalog="c",
        schema="s",
        name="orders",
        columns=[
            Column("id", Integer(), nullable=False),
            Column("name", String()),
        ],
        primary_key=["id"],
    )

    # Then primary_key returns the PK column names in declaration order
    assert table.primary_key == ("id",)


def test_delta_table_primary_key_returns_empty_when_no_pk_declared():
    # Given a DeltaTable with no PK columns
    table = DeltaTable(
        catalog="c",
        schema="s",
        name="orders",
        columns=[Column("id", Integer())],
    )

    # Then primary_key is an empty tuple
    assert table.primary_key == ()


def test_delta_table_passes_pk_to_desired_table():
    # Given a DeltaTable where "id" is PK
    table = DeltaTable(
        catalog="c",
        schema="s",
        name="orders",
        columns=[
            Column("id", Integer(), nullable=False),
            Column("ds", String()),
        ],
        primary_key=["id"],
    )

    # When converting to domain
    desired = table.to_desired_table()

    # Then primary_key is set as a value object carrying its engine-generated name
    assert desired.primary_key == PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk")


def test_delta_table_pk_column_order_matches_declaration_order():
    # Given two PK columns declared in a specific order
    table = DeltaTable(
        catalog="c",
        schema="s",
        name="orders",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("order_id", Integer(), nullable=False),
            Column("ds", String()),
        ],
        primary_key=["tenant_id", "order_id"],
    )

    # Then the order in primary_key matches declaration order
    assert table.primary_key == ("tenant_id", "order_id")


def test_delta_table_accepts_foreign_keys_parameter():
    # Given a referenced table with a primary key
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )

    # When constructing a table with a FK to it
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
    )

    # Then the FK is lowered to an internal constraint carrying its generated name
    [foreign_key] = table.to_desired_table().foreign_keys
    assert foreign_key.local_columns == ("customer_id",)
    assert foreign_key.referenced_table == QualifiedName("cat", "sch", "customers")
    assert foreign_key.referenced_columns == ("id",)
    assert foreign_key.constraint_name == "orders_customer_id_fk"


def test_delta_table_defaults_to_no_foreign_keys():
    # Given a table with no foreign_keys argument
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer())],
    )

    # Then foreign_keys defaults to an empty tuple
    assert table.foreign_keys == ()


def test_delta_table_foreign_keys_round_trip_as_declarations():
    # Given a table with a public foreign-key declaration
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )
    original = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("customer_id", Integer())],
        foreign_keys=[ForeignKey(columns="customer_id", references=customers)],
    )

    # When reusing that declaration on another table
    copy = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders_copy",
        columns=[Column("customer_id", Integer())],
        foreign_keys=original.foreign_keys,
    )

    # Then the public declarations round-trip and lowering uses the new owner
    assert copy.foreign_keys == original.foreign_keys
    [constraint] = copy.to_desired_table().foreign_keys
    assert constraint.constraint_name == "orders_copy_customer_id_fk"


def test_delta_table_rejects_fk_with_unknown_local_column():
    # Given a referenced table and a FK whose local column is not declared
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )

    # When / Then domain validation fires at construction time
    with pytest.raises(ValueError, match="nonexistent"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("id", Integer())],
            foreign_keys=[ForeignKey(columns={"nonexistent": "id"}, references=customers)],
        )


# ---------- tags ----------


def test_delta_table_passes_tags_through_to_desired_table():
    # Given a table declared with tags
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
        tags={"env": "prod", "domain": "sales"},
    )

    # When converting to the domain table
    desired = table.to_desired_table()

    # Then the tags carry through unchanged
    assert dict(desired.tags) == {"env": "prod", "domain": "sales"}


def test_delta_table_accepts_tags_as_any_mapping():
    # Given tags supplied as a read-only mapping rather than a dict
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
        tags=MappingProxyType({"owner": "data"}),
    )

    # Then the declaration copies them into the desired table
    assert dict(table.to_desired_table().tags) == {"owner": "data"}


def test_delta_table_defaults_to_no_tags():
    # Given a table with no tags argument
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
    )

    # When converting to the domain table
    desired = table.to_desired_table()

    # Then tags is an empty mapping, never None
    assert dict(desired.tags) == {}


def test_delta_table_does_not_restrict_tag_keys():
    # Given arbitrary tag keys (tags are free-form, unlike the Property allowlist)
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
        tags={"any.custom-key": "v"},
    )

    # Then construction succeeds and the key is preserved (no ValueError)
    assert dict(table.to_desired_table().tags) == {"any.custom-key": "v"}


def test_delta_table_preserves_tag_key_case():
    # Given a mixed-case tag key (UC tag keys are case-sensitive)
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
        tags={"CostCentre": "data-eng"},
    )

    # Then the key case is preserved
    assert "CostCentre" in dict(table.to_desired_table().tags)


# ---- scope


def test_delta_table_manages_all_aspects_by_default():
    # Given a table declared without a scope
    table = DeltaTable(
        catalog="dev", schema="silver", name="orders", columns=[Column("id", Integer())]
    )

    # Then the lowered desired table manages everything
    assert table.to_desired_table().managed_aspects == ALL_ASPECTS


def test_metadata_scope_manages_metadata_aspects():
    # Given a metadata-scoped declaration
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        scope="metadata",
    )

    # Then the lowered scope is exactly the metadata aspects
    assert table.to_desired_table().managed_aspects == METADATA_ASPECTS


def test_tag_scope_manages_only_table_and_column_tags():
    # Given a tag-scoped declaration of a full table shape
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[
            Column("id", Integer(), nullable=False),
            Column("email", String(), comment="PII", tags={"pii": "true"}),
        ],
        comment="Streaming orders",
        tags={"domain": "sales"},
        partitioned_by=["id"],
        primary_key=["id"],
        scope="tags",
    )

    # Then only the tag aspects are managed; the rest is carried for comparison
    desired = table.to_desired_table()
    assert desired.managed_aspects == TAG_ASPECTS
    assert desired.comment == "Streaming orders"
    assert desired.partitioned_by == ("id",)
    assert desired.primary_key_columns == ("id",)
    assert desired.tags == {"domain": "sales"}
    assert desired.columns[1].comment == "PII"
    assert desired.columns[1].tags == {"pii": "true"}


def test_delta_table_rejects_unknown_scope():
    # Given a scope value outside the named scopes
    # When / Then construction fails naming the valid options
    with pytest.raises(ValueError, match="'full', 'metadata', 'tags'"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer())],
            scope="everything",  # type: ignore[arg-type]
        )


def test_metadata_scope_carries_properties_without_deploying_them():
    # Given a metadata-scoped declaration of a full table, properties included —
    # the scope restricts deployment, not what may be declared
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        properties={Property.CHANGE_DATA_FEED.value: "true"},
        scope="metadata",
    )

    # Then the declaration carries the property; PROPERTIES stays unmanaged
    desired = table.to_desired_table()
    assert desired.properties == {Property.CHANGE_DATA_FEED.value: "true"}
    assert TableAspect.PROPERTIES not in desired.managed_aspects


def test_tag_scope_carries_foreign_keys_without_managing_them():
    # Given a tag-scoped declaration that mirrors the live table's foreign key
    customers = DeltaTable(
        catalog="dev",
        schema="silver",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[
            Column("id", Integer(), nullable=False),
            Column("customer_id", Integer()),
        ],
        primary_key=["id"],
        foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        scope="tags",
    )

    # Then the declaration carries the key; FOREIGN_KEYS stays unmanaged
    desired = table.to_desired_table()
    assert len(desired.foreign_keys) == 1
    assert TableAspect.FOREIGN_KEYS not in desired.managed_aspects


def test_metadata_scope_without_properties_constructs_cleanly():
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        scope="metadata",
    )

    assert table.to_desired_table().properties == {}


def test_no_properties_are_injected_by_default():
    # Given a table declared without properties
    table = DeltaTable(
        catalog="coredev",
        schema="medallia",
        name="responses",
        columns=[Column("id", Integer())],
    )

    # Then the desired table carries exactly what was declared: nothing
    assert table.to_desired_table().properties == {}


def test_none_property_declarations_are_carried_through():
    # Given a declaration asserting a key absent
    table = DeltaTable(
        catalog="coredev",
        schema="medallia",
        name="responses",
        columns=[Column("id", Integer())],
        properties={Property.CHANGE_DATA_FEED.value: None},
    )

    assert table.to_desired_table().properties == {Property.CHANGE_DATA_FEED.value: None}


def test_rejects_unmanaged_key_declared_as_none():
    # Given an absence assertion for a key the engine does not manage
    with pytest.raises(ValueError, match="not managed"):
        DeltaTable(
            catalog="coredev",
            schema="medallia",
            name="responses",
            columns=[Column("id", Integer())],
            properties={"delta.enableRowTracking": None},
        )


def test_delta_table_rejects_invalid_property_value() -> None:
    with pytest.raises(ValueError, match=r"delta\.enableChangeDataFeed"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer())],
            properties={"delta.enableChangeDataFeed": "yes"},
        )


def test_delta_table_accepts_none_property_value_without_value_check() -> None:
    # None asserts absence; it is not a value and must not be validated as one.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        properties={"delta.enableChangeDataFeed": None},
    )
    assert table.to_desired_table().properties["delta.enableChangeDataFeed"] is None


def test_metadata_scope_mirrors_cdf_reserved_columns_with_cdf_declared() -> None:
    # A metadata-scoped declaration does not manage properties, so the
    # declaration is a mirror of existing state, not an attempt to enable CDF.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer()), Column("_change_type", String())],
        properties={"delta.enableChangeDataFeed": "true"},
        scope="metadata",
    )
    assert len(table.to_desired_table().columns) == 2


def test_tag_scope_mirrors_cdf_reserved_columns_with_cdf_declared() -> None:
    # A tag-scoped declaration does not manage column structure either, so it
    # must mirror reserved names the live table already carries.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer()), Column("_change_type", String())],
        properties={"delta.enableChangeDataFeed": "true"},
        scope="tags",
    )
    assert len(table.to_desired_table().columns) == 2


def test_delta_table_accepts_column_tags_at_the_limits() -> None:
    # A column is its own securable: 50 tags of 256 characters are accepted
    at_limit = {f"tag_{i}": "x" * 256 for i in range(50)}
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer(), tags=at_limit)],
    )
    assert len(table.to_desired_table().columns[0].tags) == 50


def test_metadata_scope_still_lowers_the_full_schema():
    # Given a metadata-scoped declaration with full schema detail
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer(), nullable=False), Column("name", String())],
        scope="metadata",
    )

    # Then all columns are lowered — scope controls reconciliation, not lowering
    desired = table.to_desired_table()
    assert tuple(c.name for c in desired.columns) == ("id", "name")


def test_metadata_aspects_excludes_existence_and_physical_aspects():
    # Given the metadata named scope
    # Then physical-behaviour aspects are excluded by design
    assert METADATA_ASPECTS == ALL_ASPECTS - frozenset(
        {
            TableAspect.TABLE_EXISTENCE,
            TableAspect.COLUMN_STRUCTURE,
            TableAspect.PROPERTIES,
            TableAspect.PARTITIONING,
            TableAspect.CLUSTERING,
        }
    )


def test_tag_aspects_contains_only_tag_aspects():
    # Given the tags named scope
    # Then it manages exactly table and column tags
    assert TAG_ASPECTS == frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS})


def test_delta_table_rejects_special_character_column_names_without_column_mapping() -> None:
    with pytest.raises(ValueError, match="columnMapping"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("order id", Integer())],
        )


def test_delta_table_accepts_special_character_column_names_with_column_mapping() -> None:
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("order id", Integer())],
        properties={"delta.columnMapping.mode": "name"},
    )
    assert table.to_desired_table().columns[0].name == "order id"


def test_delta_table_rejects_special_character_struct_field_names_without_column_mapping() -> None:
    with pytest.raises(ValueError, match="columnMapping"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[
                Column("payload", Struct((StructField("order id", Integer()),))),
            ],
        )


def test_delta_table_accepts_special_character_struct_field_names_with_column_mapping() -> None:
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[
            Column("payload", Struct((StructField("order id", Integer()),))),
        ],
        properties={"delta.columnMapping.mode": "name"},
    )
    [column] = table.to_desired_table().columns
    assert column.data_type == Struct((StructField("order id", Integer()),))


def test_delta_table_rejects_special_character_field_names_in_struct_nested_in_array() -> None:
    # Proves the gate recurses through container types, not just direct struct columns.
    with pytest.raises(ValueError, match="columnMapping"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[
                Column(
                    "items",
                    Array(Struct((StructField("order id", Integer()),))),
                ),
            ],
        )


def test_metadata_scope_mirrors_special_character_columns_without_the_property() -> None:
    # A metadata-scoped declaration never creates or adds columns; the catalog
    # already accepted this name, so the declaration must be able to mirror it.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("order id", Integer())],
        scope="metadata",
    )
    assert table.to_desired_table().columns[0].name == "order id"


def test_tag_scope_mirrors_special_character_columns_without_column_mapping() -> None:
    # A tag-scoped declaration never creates or adds columns either.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("order id", Integer())],
        scope="tags",
    )
    assert table.to_desired_table().columns[0].name == "order id"


def test_delta_table_rejects_cdf_reserved_column_names_when_cdf_enabled() -> None:
    with pytest.raises(ValueError, match="_change_type"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer()), Column("_change_type", String())],
            properties={"delta.enableChangeDataFeed": "true"},
        )


def test_delta_table_accepts_cdf_reserved_column_names_when_cdf_not_enabled() -> None:
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer()), Column("_change_type", String())],
    )
    assert len(table.to_desired_table().columns) == 2


# ---- tag limits (Unity Catalog) ----


def test_delta_table_rejects_more_than_fifty_table_tags() -> None:
    too_many = {f"tag_{i}": "v" for i in range(51)}
    with pytest.raises(ValueError, match="50"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer())],
            tags=too_many,
        )


def test_delta_table_rejects_overlong_tag_value_on_a_column() -> None:
    with pytest.raises(ValueError, match="256"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer(), tags={"note": "x" * 257})],
        )


def test_delta_table_rejects_overlong_tag_key_on_a_column() -> None:
    with pytest.raises(ValueError, match="256"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer(), tags={"x" * 257: "note"})],
        )


def test_delta_table_accepts_a_tag_key_at_the_length_limit() -> None:
    # 256 is the accepted boundary, so the guard must reject only beyond it
    at_limit = "k" * 256
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer(), tags={at_limit: "note"})],
    )
    assert at_limit in table.to_desired_table().columns[0].tags


def test_delta_table_accepts_tags_at_the_limits() -> None:
    at_limit = {f"tag_{i}": "x" * 256 for i in range(50)}
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        tags=at_limit,
    )
    assert len(table.to_desired_table().tags) == 50


def test_delta_table_rejects_rename_hint_without_column_mapping():
    with pytest.raises(ValueError, match="columnMapping"):
        DeltaTable(
            "dev",
            "silver",
            "customers",
            columns=[
                Column("id", Integer(), nullable=False),
                Column("customer_name", String(), renamed_from="customer_nm"),
            ],
        )


def test_delta_table_accepts_rename_hint_with_column_mapping_declared():
    table = DeltaTable(
        "dev",
        "silver",
        "customers",
        columns=[
            Column("id", Integer(), nullable=False),
            Column("customer_name", String(), renamed_from="customer_nm"),
        ],
        properties={"delta.columnMapping.mode": "name"},
    )
    assert table.columns[1].renamed_from == "customer_nm"


def test_single_column_string_foreign_key_infers_and_normalizes_parent_column():
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("ID", Integer(), nullable=False)],
        primary_key=["ID"],
    )
    declaration = ForeignKey(columns="Customer_ID", references=customers)

    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("Customer_ID", Integer())],
        foreign_keys=[declaration],
    )

    [foreign_key] = orders.to_desired_table().foreign_keys

    assert declaration.columns == ("customer_id",)
    assert foreign_key.local_columns == ("customer_id",)
    assert foreign_key.referenced_columns == ("id",)


def test_single_column_sequence_infers_and_normalizes_parent_column():
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("ID", Integer(), nullable=False)],
        primary_key=["ID"],
    )
    declaration = ForeignKey(columns=["Customer_ID"], references=customers)

    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("Customer_ID", Integer())],
        foreign_keys=[declaration],
    )

    [foreign_key] = orders.to_desired_table().foreign_keys

    assert declaration.columns == ("customer_id",)
    assert foreign_key.local_columns == ("customer_id",)
    assert foreign_key.referenced_columns == ("id",)


def test_same_name_composite_foreign_key_is_inferred_by_normalized_name():
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("Tenant_ID", Integer(), nullable=False),
            Column("Account_ID", Integer(), nullable=False),
        ],
        primary_key=["Tenant_ID", "Account_ID"],
    )

    entries = DeltaTable(
        catalog="cat",
        schema="sch",
        name="entries",
        columns=[
            Column("Account_ID", Integer()),
            Column("Tenant_ID", Integer()),
        ],
        foreign_keys=[
            ForeignKey(
                # Deliberately reversed relative to the parent's declaration.
                columns=["Account_ID", "Tenant_ID"],
                references=accounts,
            )
        ],
    )

    [foreign_key] = entries.to_desired_table().foreign_keys

    assert foreign_key.local_columns == ("account_id", "tenant_id")
    assert foreign_key.referenced_columns == ("account_id", "tenant_id")


def test_ambiguous_composite_foreign_key_requires_mapping():
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )

    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("tenant_id", Integer()),
                Column("customer_id", Integer()),
            ],
            foreign_keys=[
                ForeignKey(
                    columns=["tenant_id", "customer_id"],
                    references=accounts,
                )
            ],
        )


def _customers() -> DeltaTable:
    return DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )


def test_foreign_key_declaration_has_only_columns_and_references():
    # Given / When / Then the public declaration rejects internal fields
    with pytest.raises(TypeError):
        ForeignKey(  # type: ignore[call-arg]
            columns={"customer_id": "id"},
            references=_customers(),
            referenced_columns=("id",),
        )
    with pytest.raises(TypeError):
        ForeignKey(  # type: ignore[call-arg]
            columns={"customer_id": "id"},
            references=_customers(),
            constraint_name="orders_customer_id_fk",
        )


def test_delta_table_stores_composite_foreign_key_canonically():
    # Given a referenced table with a composite primary key (tenant_id, id)
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[
            Column("tenant_id", Integer()),
            Column("customer_id", Integer()),
        ],
        foreign_keys=[
            ForeignKey(
                columns={"tenant_id": "tenant_id", "customer_id": "id"},
                references=customers,
            )
        ],
    )

    # Then pairs are stored canonically (sorted by local column), pairing intact
    [foreign_key] = orders.to_desired_table().foreign_keys
    assert foreign_key.local_columns == ("customer_id", "tenant_id")
    assert foreign_key.referenced_columns == ("id", "tenant_id")


def test_delta_table_supports_self_referential_foreign_key():
    # Given a table referencing its own primary key via the Self sentinel
    employee = DeltaTable(
        catalog="cat",
        schema="sch",
        name="employee",
        columns=[
            Column("id", Integer(), nullable=False),
            Column("manager_id", Integer()),
        ],
        primary_key=["id"],
        foreign_keys=[ForeignKey(columns={"manager_id": "id"}, references=Self)],
    )

    # Then the FK targets the table's own qualified name and primary key
    [foreign_key] = employee.to_desired_table().foreign_keys
    assert foreign_key.referenced_table == QualifiedName("cat", "sch", "employee")
    assert foreign_key.referenced_columns == ("id",)


def test_delta_table_rejects_reference_to_table_with_no_primary_key():
    # Given a referenced table with no primary key
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer())],
    )

    # When / Then construction fails because there is no key to infer
    with pytest.raises(ValueError, match="no primary key"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("id", Integer()), Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def test_delta_table_rejects_self_reference_without_primary_key():
    # Given a table with no primary key that references itself
    # When / Then the same no-primary-key error names the table
    with pytest.raises(ValueError, match="no primary key"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="employee",
            columns=[Column("manager_id", Integer())],
            foreign_keys=[ForeignKey(columns={"manager_id": "id"}, references=Self)],
        )


def test_delta_table_rejects_cross_catalog_foreign_key():
    # Given a referenced table that lives in a different catalog
    customers = DeltaTable(
        catalog="other",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )

    # When / Then the declaration is rejected: information_schema is
    # per-catalog, so the engine could create the constraint but never
    # observe it, and every later sync would re-plan and fail.
    with pytest.raises(ValueError, match="cross-catalog"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def test_delta_table_rejects_foreign_keys_whose_generated_names_collide():
    # Given two FKs over different local columns whose generated constraint
    # names collide: ('a', 'b_c') and ('a_b', 'c') both derive orders_a_b_c_fk
    parts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="parts",
        columns=[
            Column("x", Integer(), nullable=False),
            Column("y", Integer(), nullable=False),
        ],
        primary_key=["x", "y"],
    )

    # When / Then the collision is rejected at declaration time
    with pytest.raises(ValueError, match="same constraint name"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("a", Integer()),
                Column("b_c", Integer()),
                Column("a_b", Integer()),
                Column("c", Integer()),
            ],
            foreign_keys=[
                ForeignKey(columns={"a": "x", "b_c": "y"}, references=parts),
                ForeignKey(columns={"a_b": "x", "c": "y"}, references=parts),
            ],
        )


def test_delta_table_rejects_non_table_reference():
    # Given a reference that is neither a DeltaTable nor Self
    # When / Then a TypeError names the accepted types
    with pytest.raises(TypeError, match="DeltaTable or Self"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("customer_id", Integer())],
            foreign_keys=[
                ForeignKey(columns={"customer_id": "id"}, references="cat.sch.customers")  # type: ignore[arg-type]
            ],
        )


def test_foreign_key_rejects_local_column_type_mismatch_with_target_primary_key():
    # Given a referenced primary key of type Long and a local column of type String
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Long(), nullable=False)],
        primary_key=["id"],
    )

    # When / Then construction fails because the local column type does not match
    with pytest.raises(ValueError, match="type mismatch"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("id", Long(), nullable=False),
                Column("customer_id", String()),
            ],
            primary_key=["id"],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def test_self_referential_foreign_key_rejects_type_mismatch():
    # Given a self-referential foreign key whose local column type differs from the primary key
    # When / Then construction fails because the local column type does not match
    with pytest.raises(ValueError, match="type mismatch"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="employees",
            columns=[
                Column("id", Long(), nullable=False),
                Column("manager_id", Integer()),
            ],
            primary_key=["id"],
            foreign_keys=[ForeignKey(columns={"manager_id": "id"}, references=Self)],
        )


def test_composite_foreign_key_rejects_a_single_mismatched_column_pair():
    # Given a composite referenced primary key where only the second local
    # column's type differs
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Long(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )

    # When / Then construction fails naming the mismatched pair
    with pytest.raises(ValueError, match=r"orders\.customer_id"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("id", Long(), nullable=False),
                Column("customer_tenant_id", Integer()),
                Column("customer_id", String()),
            ],
            primary_key=["id"],
            foreign_keys=[
                ForeignKey(
                    columns={"customer_tenant_id": "tenant_id", "customer_id": "id"},
                    references=customers,
                )
            ],
        )


def test_foreign_key_with_matching_types_still_lowers():
    # Given a referenced primary key and local column that share the same type
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Long(), nullable=False)],
        primary_key=["id"],
    )
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[
            Column("id", Long(), nullable=False),
            Column("customer_id", Long()),
        ],
        primary_key=["id"],
        foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
    )

    # Then the foreign key lowers normally
    assert orders.to_desired_table().foreign_keys[0].local_columns == ("customer_id",)


def test_foreign_key_accepts_columns_as_any_mapping():
    # Given columns supplied as a read-only mapping rather than a dict
    customers = _customers()
    declaration = ForeignKey(columns=MappingProxyType({"Customer_ID": "ID"}), references=customers)
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[declaration],
    )

    # Then the declaration copies the mapping and the lowered constraint holds an immutable tuple
    assert dict(declaration.columns) == {"customer_id": "id"}
    [constraint] = orders.to_desired_table().foreign_keys
    assert constraint.local_columns == ("customer_id",)


def test_mapping_lowers_single_column_foreign_key():
    customers = _customers()
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("customer_id", Integer())],
        foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
    )

    [foreign_key] = orders.to_desired_table().foreign_keys
    assert foreign_key.local_columns == ("customer_id",)
    assert foreign_key.referenced_table == QualifiedName("cat", "sch", "customers")
    assert foreign_key.referenced_columns == ("id",)
    assert foreign_key.constraint_name == "orders_customer_id_fk"


def test_mapping_lowers_composite_foreign_key_with_stated_pairing():
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("tenant_id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[
            ForeignKey(
                columns={"tenant_id": "tenant_id", "customer_id": "id"},
                references=accounts,
            )
        ],
    )

    # Stored canonically (sorted by local column), pairing exactly as stated
    [foreign_key] = orders.to_desired_table().foreign_keys
    assert foreign_key.local_columns == ("customer_id", "tenant_id")
    assert foreign_key.referenced_columns == ("id", "tenant_id")


def test_mapping_insertion_order_is_irrelevant():
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )

    def orders_with(mapping):
        table = DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("tenant_id", Integer()), Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns=mapping, references=accounts)],
        )
        return table.to_desired_table().foreign_keys[0]

    one = orders_with({"tenant_id": "tenant_id", "customer_id": "id"})
    two = orders_with({"customer_id": "id", "tenant_id": "tenant_id"})
    assert one == two
    assert one.constraint_name == two.constraint_name


def test_mapping_not_covering_the_key_is_rejected():
    # Given a mapping missing one key column and naming a non-key column
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
            Column("region", Integer()),
        ],
        primary_key=["tenant_id", "id"],
    )

    with pytest.raises(ValueError) as excinfo:
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("tenant_id", Integer()), Column("region_ref", Integer())],
            foreign_keys=[
                ForeignKey(
                    columns={"tenant_id": "tenant_id", "region_ref": "region"},
                    references=accounts,
                )
            ],
        )

    # Both sides named: what's missing from the mapping, what isn't in the key
    assert "id" in str(excinfo.value)
    assert "region" in str(excinfo.value)


def test_two_locals_mapped_to_the_same_key_column_are_rejected():
    with pytest.raises(ValueError, match="Duplicate foreign key referenced column"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("a", Integer()), Column("b", Integer())],
            foreign_keys=[ForeignKey(columns={"a": "id", "b": "id"}, references=_customers())],
        )


def test_reordering_the_parent_primary_key_produces_no_foreign_key_drift():
    # Regression for the parent-reorder trap: the child's mapping is explicit,
    # so a parent primary_key list reorder must be a no-op end to end.
    from delta_engine.domain.plan.diff import TableDrift, diff_table

    def child_of(parent_key_order):
        accounts = DeltaTable(
            catalog="cat",
            schema="sch",
            name="accounts",
            columns=[
                Column("tenant_id", Integer(), nullable=False),
                Column("id", Integer(), nullable=False),
            ],
            primary_key=parent_key_order,
        )
        return DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("tenant_id", Integer()), Column("customer_id", Integer())],
            foreign_keys=[
                ForeignKey(
                    columns={"tenant_id": "tenant_id", "customer_id": "id"},
                    references=accounts,
                )
            ],
        ).to_desired_table()

    before = child_of(["tenant_id", "id"])
    after = child_of(["id", "tenant_id"])
    assert before.foreign_keys == after.foreign_keys

    from delta_engine.domain.model import ObservedTable
    from tests.builders import as_observed_columns

    observed = ObservedTable(
        qualified_name=before.qualified_name,
        columns=as_observed_columns(before.columns),
        foreign_keys=before.foreign_keys,
    )
    diff = diff_table(after, observed)
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.unresolvable == ()
