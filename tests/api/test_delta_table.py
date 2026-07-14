from types import MappingProxyType

import pytest

from delta_engine.api.delta_table import METADATA_ASPECTS, TAG_ASPECTS
from delta_engine.domain.model import (
    ALL_ASPECTS,
    DesiredColumn as DomainColumn,
    QualifiedName,
    TableAspect,
)
from delta_engine.domain.model.constraints import PrimaryKeyConstraint
from delta_engine.schema import (
    Array,
    Column,
    Date,
    DeltaTable,
    ForeignKey,
    Integer,
    Map,
    Property,
    String,
    Struct,
    StructField,
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
    [foreign_key] = table.foreign_keys
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


def test_rejects_unregistered_key_declared_as_none():
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
