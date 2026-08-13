from types import MappingProxyType

import pytest

from delta_engine.domain.model import (
    DesiredColumn as DomainColumn,
    QualifiedName,
    TableAspect,
    TableScope,
)
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


def test_mixed_case_declaration_preserves_columns_and_lowercases_object_names():
    # Unity Catalog stores catalog/schema/table names lowercase (live-pinned);
    # column spelling is catalog display state and is preserved.
    table = DeltaTable(
        catalog="Main",
        schema="Sales",
        name="Orders",
        columns=[Column("Id", Integer())],
    )

    assert (table.catalog, table.schema, table.name) == ("main", "sales", "orders")
    assert [str(column.name) for column in table.columns] == ["Id"]


def test_delta_table_rejects_columns_differing_only_by_case_as_duplicates() -> None:
    # The public API delegates to the same identity-keyed validation as the
    # domain layer: a declaration cannot carry two spellings of one column.
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column("Id", Integer()), Column("ID", Integer())],
        )


def test_public_accessors_return_column_spelling_for_references():
    # Given references whose casing differs from their columns
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("OrderId", Integer(), nullable=False), Column("Region", String())],
        clustered_by=["REGION"],
        primary_key=["ORDERID"],
    )

    # Then attached references use the columns' spelling
    assert tuple(str(column) for column in table.primary_key) == ("OrderId",)
    assert tuple(str(column) for column in table.clustered_by) == ("Region",)


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
    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column("id", Integer()), Column("region", String())],
            partitioned_by=["id"],
            clustered_by=["region"],
        )


def test_delta_table_rejects_more_than_four_clustering_keys():
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column(name, Integer()) for name in ("a", "b", "c", "d", "e")],
            clustered_by=["a", "b", "c", "d", "e"],
        )


def test_delta_table_rejects_complex_typed_clustering_column():
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column("id", Integer()), Column("attrs", Map(String(), String()))],
            clustered_by=["attrs"],
        )


@pytest.mark.parametrize(
    "layout",
    [
        {"partitioned_by": ["ITEMS"]},
        {"clustered_by": ["ITEMS"]},
    ],
)
def test_layout_keys_are_normalized_before_type_validation(layout):
    # Column declarations are already canonical lowercase. Layout identifiers
    # must use the same representation before API policy tries to resolve them.
    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
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
        ["foo", "bar.baz"],  # multiple unknown keys at once
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
    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer()), Column("day", String())],
            partitioned_by=["id", "day"],
        )


def test_partition_keys_are_normalized_before_whole_table_validation() -> None:
    with pytest.raises(ValueError):
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

    # When lowering the declaration
    desired = table.to_desired_table()

    # Then the physical name is generated once the owning table is known
    assert desired.primary_key is not None
    assert desired.primary_key.columns == ("tenant_id", "id")
    assert desired.primary_key.requested_name == "accounts_pk"
    assert table.primary_key_name == "accounts_pk"


def test_primary_key_name_is_preserved_as_explicit_managed_state():
    # Given a primary key with an explicit physical name
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
        primary_key_name="Accounts_Business_Key",
    )

    # When reading the declaration's managed primary-key name
    desired = table.to_desired_table()

    # Then both the public API and lowered state preserve its spelling
    assert desired.primary_key is not None
    assert desired.primary_key.requested_name == "Accounts_Business_Key"
    assert table.primary_key_name == "Accounts_Business_Key"


def test_no_primary_key_parameter_means_no_key():
    # Given a declaration without a primary key
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="events",
        columns=[Column("id", Integer())],
    )

    # Then neither key columns nor a physical key name are exposed
    assert table.to_desired_table().primary_key is None
    assert table.primary_key == ()
    assert table.primary_key_name is None


def test_primary_key_name_requires_a_primary_key():
    # When a physical name is supplied without a key, then the declaration is rejected
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="events",
            columns=[Column("id", Integer())],
            primary_key_name="events_pk",
        )


@pytest.mark.parametrize(
    ("invalid_name", "expected_error"),
    [
        pytest.param("  ", ValueError, id="blank"),
        pytest.param(42, TypeError, id="not-a-string"),
    ],
)
def test_primary_key_name_rejects_invalid_values(
    invalid_name: object,
    expected_error: type[Exception],
):
    # Given an otherwise valid primary-key declaration
    kwargs = {
        "catalog": "cat",
        "schema": "sch",
        "name": "events",
        "columns": [Column("id", Integer(), nullable=False)],
        "primary_key": ["id"],
    }

    # When the physical name is invalid, then construction fails deliberately
    with pytest.raises(expected_error):
        DeltaTable(**kwargs, primary_key_name=invalid_name)  # type: ignore[arg-type]


def test_empty_primary_key_sequence_is_rejected():
    # Given an empty sequence — a meaningless assertion, distinct from None
    with pytest.raises(ValueError):
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
    with pytest.raises(TypeError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="events",
            columns=[Column("i", Integer(), nullable=False)],
            primary_key="i",
        )


def test_primary_key_naming_unknown_column_is_rejected():
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="events",
            columns=[Column("id", Integer(), nullable=False)],
            primary_key=["missing_col"],
        )


def test_primary_key_over_nullable_column_is_rejected():
    with pytest.raises(ValueError):
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
    assert foreign_key.requested_name == "orders_customer_id_fk"


def test_foreign_key_name_is_preserved_as_explicit_managed_state():
    # Given a foreign key with an explicit physical name
    customers = _customers()
    declaration = ForeignKey(
        columns="customer_id",
        references=customers,
        name="Orders_Customer_Relationship",
    )

    # When the declaration is attached to its owning table
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("customer_id", Integer())],
        foreign_keys=[declaration],
    )

    # Then the public declaration and domain constraint preserve its spelling
    assert declaration.name == "Orders_Customer_Relationship"
    [constraint] = orders.to_desired_table().foreign_keys
    assert str(constraint.requested_name) == "Orders_Customer_Relationship"


@pytest.mark.parametrize(
    ("invalid_name", "expected_error"),
    [
        pytest.param("  ", ValueError, id="blank"),
        pytest.param(42, TypeError, id="not-a-string"),
    ],
)
def test_foreign_key_name_rejects_invalid_values(
    invalid_name: object,
    expected_error: type[Exception],
):
    # When the physical name is invalid, then the ForeignKey declaration rejects it
    with pytest.raises(expected_error):
        ForeignKey(
            columns="customer_id",
            references=_customers(),
            name=invalid_name,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("columns", "expected_error"),
    [
        pytest.param([], ValueError, id="empty-sequence"),
        pytest.param({}, ValueError, id="empty-mapping"),
        pytest.param({"customer_id"}, TypeError, id="unsupported-collection"),
        pytest.param(["customer_id", 42], TypeError, id="non-string-sequence-entry"),
        pytest.param({"customer_id": 42}, TypeError, id="non-string-mapping-value"),
        pytest.param({42: "id"}, TypeError, id="non-string-mapping-key"),
        pytest.param("  ", ValueError, id="blank-string"),
        pytest.param(["  "], ValueError, id="blank-sequence-entry"),
        pytest.param({"customer_id": "  "}, ValueError, id="blank-mapping-entry"),
    ],
)
def test_foreign_key_rejects_invalid_column_input(
    columns: object,
    expected_error: type[Exception],
) -> None:
    # When column syntax is invalid, then ForeignKey rejects it without needing an owner
    with pytest.raises(expected_error):
        ForeignKey(
            columns=columns,  # type: ignore[arg-type]
            references=_customers(),
        )


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
    assert constraint.requested_name == "orders_copy_customer_id_fk"


def test_delta_table_rejects_fk_with_unknown_local_column():
    # Given a referenced table and a FK whose local column is not declared
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )

    # When the owner is constructed, then domain validation rejects the missing column
    with pytest.raises(ValueError):
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

# Every scope below "full" leaves column structure unmanaged, which is what
# exempts a declaration from the naming rules that bind a table's author.
_SCOPES_WITHOUT_COLUMN_STRUCTURE = ["metadata", "annotations", "tags"]


def test_delta_table_manages_all_aspects_by_default():
    # Given a table declared without a scope
    table = DeltaTable(
        catalog="dev", schema="silver", name="orders", columns=[Column("id", Integer())]
    )

    # Then the lowered desired table manages everything
    assert table.to_desired_table().scope is TableScope.FULL


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        ("full", TableScope.FULL),
        ("metadata", TableScope.METADATA),
        ("annotations", TableScope.ANNOTATIONS),
        ("tags", TableScope.TAGS),
    ],
)
def test_a_scope_name_narrows_what_the_declaration_manages(scope, expected):
    # Given a declaration carrying a named scope
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        scope=scope,
    )

    # Then the public name becomes the corresponding closed domain scope.
    assert table.to_desired_table().scope is expected


def test_a_restricted_scope_still_lowers_everything_declared():
    # Given a tag-scoped declaration of a full table shape — the narrowest
    # scope, so everything but the tags is outside what it manages
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

    # Then it is all lowered anyway. Scope decides what gets reconciled, not
    # what may be declared: the unmanaged detail is what drift is judged
    # against, so dropping it would leave nothing to refuse
    desired = table.to_desired_table()
    assert desired.comment == "Streaming orders"
    assert desired.partitioned_by == ("id",)
    assert desired.primary_key_columns == ("id",)
    assert desired.tags == {"domain": "sales"}
    assert desired.columns[1].comment == "PII"
    assert desired.columns[1].tags == {"pii": "true"}


def test_delta_table_rejects_unknown_scope():
    # Given a scope value outside the named scopes
    # When the table is constructed, then the unknown scope is rejected
    with pytest.raises(ValueError):
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
    assert not desired.scope.manages(TableAspect.PROPERTIES)


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
    assert not desired.scope.manages(TableAspect.FOREIGN_KEYS)


def test_annotations_scope_carries_a_mirrored_primary_key_without_managing_it():
    # Given an annotations-scoped declaration restating a key that a streaming
    # table's pipeline declared — the contract the guide asks for
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="clicks",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
        scope="annotations",
    )

    # Then the key is lowered but PRIMARY_KEY stays unmanaged: it is carried so
    # that the diff finds nothing to act on, never so that it can be applied.
    # Omitting it would instead read as a drop this scope cannot authorise
    desired = table.to_desired_table()
    assert desired.primary_key is not None
    assert not desired.scope.manages(TableAspect.PRIMARY_KEY)


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
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="coredev",
            schema="medallia",
            name="responses",
            columns=[Column("id", Integer())],
            properties={"delta.enableRowTracking": None},
        )


def test_delta_table_rejects_invalid_property_value() -> None:
    with pytest.raises(ValueError):
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


@pytest.mark.parametrize("scope", _SCOPES_WITHOUT_COLUMN_STRUCTURE)
def test_a_restricted_scope_mirrors_cdf_reserved_column_names(scope) -> None:
    # A declaration that does not manage column structure never creates or adds
    # a column, so naming one is mirroring state the catalog already holds. It
    # carries the CDF property for the same reason, not to enable the feature.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer()), Column("_change_type", String())],
        properties={"delta.enableChangeDataFeed": "true"},
        scope=scope,
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


def test_delta_table_rejects_special_character_column_names_without_column_mapping() -> None:
    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
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


@pytest.mark.parametrize(
    ("data_type", "column_nullable", "is_valid"),
    [
        (
            Struct(
                (
                    StructField(
                        "nested",
                        Struct((StructField("value", Integer(), nullable=False),)),
                        nullable=False,
                    ),
                )
            ),
            False,
            True,
        ),
        (Struct((StructField("value", Integer(), nullable=False),)), True, False),
        (
            Struct(
                (
                    StructField(
                        "nested",
                        Struct((StructField("value", Integer(), nullable=False),)),
                    ),
                )
            ),
            False,
            False,
        ),
        (
            Array(Struct((StructField("value", Integer(), nullable=False),))),
            False,
            False,
        ),
        (
            Map(Struct((StructField("value", Integer(), nullable=False),)), String()),
            False,
            False,
        ),
        (
            Map(String(), Struct((StructField("value", Integer(), nullable=False),))),
            False,
            False,
        ),
    ],
    ids=[
        "valid",
        "nullable-column",
        "nullable-parent-field",
        "array",
        "map-key",
        "map-value",
    ],
)
def test_delta_table_validates_non_nullable_struct_field_placement(
    data_type, column_nullable: bool, is_valid: bool
) -> None:
    column = Column("payload", data_type, nullable=column_nullable)
    if not is_valid:
        with pytest.raises(ValueError):
            DeltaTable("dev", "silver", "orders", columns=[column])
        return

    table = DeltaTable("dev", "silver", "orders", columns=[column])
    assert table.to_desired_table().columns[0].data_type == data_type


def test_delta_table_rejects_special_character_field_names_in_struct_nested_in_array() -> None:
    # Proves the gate recurses through container types, not just direct struct columns.
    with pytest.raises(ValueError):
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


@pytest.mark.parametrize("scope", _SCOPES_WITHOUT_COLUMN_STRUCTURE)
def test_a_restricted_scope_only_relaxes_catalog_dependent_column_validation(scope) -> None:
    # Restricted scopes can omit the property that made an observed column
    # name valid, but they still reject nullability no Delta schema can retain.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("order id", Integer())],
        scope=scope,
    )
    assert [column.name for column in table.to_desired_table().columns] == ["order id"]

    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[
                Column(
                    "items",
                    Array(Struct((StructField("value", Integer(), nullable=False),))),
                )
            ],
            scope=scope,
        )


def test_delta_table_rejects_cdf_reserved_column_names_when_cdf_enabled() -> None:
    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer())],
            tags=too_many,
        )


def test_delta_table_rejects_overlong_tag_value_on_a_column() -> None:
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer(), tags={"note": "x" * 257})],
        )


def test_delta_table_rejects_overlong_tag_key_on_a_column() -> None:
    with pytest.raises(ValueError):
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
    with pytest.raises(ValueError):
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


def test_single_column_string_foreign_key_infers_and_preserves_spelling():
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

    assert declaration.columns == ("Customer_ID",)
    assert tuple(str(c) for c in foreign_key.local_columns) == ("Customer_ID",)
    assert tuple(str(c) for c in foreign_key.referenced_columns) == ("ID",)


def test_single_column_sequence_infers_and_preserves_spelling():
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

    assert declaration.columns == ("Customer_ID",)
    assert tuple(str(c) for c in foreign_key.local_columns) == ("Customer_ID",)
    assert tuple(str(c) for c in foreign_key.referenced_columns) == ("ID",)


def test_same_name_composite_foreign_key_is_paired_by_name_not_declared_order():
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

    assert foreign_key.local_columns == ("Account_ID", "Tenant_ID")
    assert foreign_key.referenced_columns == ("Account_ID", "Tenant_ID")


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


def test_foreign_key_declaration_rejects_internal_constraint_fields():
    # When lowered domain fields are supplied, then the public declaration rejects them
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
            requested_name="orders_customer_id_fk",
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

    # When the child is constructed, then the missing target key prevents inference
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("id", Integer()), Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def test_delta_table_rejects_self_reference_without_primary_key():
    # When a table without a primary key references itself, then construction fails
    with pytest.raises(ValueError):
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

    # When the child is declared, then the cross-catalog relationship is rejected:
    # information_schema is
    # per-catalog, so the engine could create the constraint but never
    # observe it, and every later sync would re-plan and fail.
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def _table_with_colliding_generated_foreign_key_names(
    names: tuple[str | None, str | None] = (None, None),
) -> DeltaTable:
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
    return DeltaTable(
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
            ForeignKey(
                columns={"a": "x", "b_c": "y"},
                references=parts,
                name=names[0],
            ),
            ForeignKey(
                columns={"a_b": "x", "c": "y"},
                references=parts,
                name=names[1],
            ),
        ],
    )


def test_delta_table_rejects_foreign_keys_whose_generated_names_collide():
    # Given two FKs whose distinct column sets both derive orders_a_b_c_fk

    # When both are declared, then the collision is rejected
    with pytest.raises(ValueError):
        _table_with_colliding_generated_foreign_key_names()


def test_explicit_name_disambiguates_generated_foreign_key_name_collision():
    # Given two structurally distinct FKs whose generated names would collide

    # When one declaration supplies a distinct physical name
    orders = _table_with_colliding_generated_foreign_key_names((None, "orders_parts_two_fk"))

    # Then construction succeeds with one generated and one explicit name
    assert tuple(
        str(constraint.requested_name) for constraint in orders.to_desired_table().foreign_keys
    ) == ("orders_a_b_c_fk", "orders_parts_two_fk")


def test_foreign_key_rejects_non_table_reference_during_its_construction():
    # When the reference is neither a DeltaTable nor Self, then ForeignKey rejects it
    with pytest.raises(TypeError):
        ForeignKey(
            columns={"customer_id": "id"},
            references="cat.sch.customers",  # type: ignore[arg-type]
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

    # When the child is constructed, then the local-column type mismatch is rejected
    with pytest.raises(ValueError):
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
    # When a self-reference has a different local type, then construction fails
    with pytest.raises(ValueError):
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

    # When the child is constructed, then the incompatible pair is rejected
    with pytest.raises(ValueError):
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

    # Then the declaration copies the mapping and the attached constraint uses
    # the actual columns' spelling on both sides.
    assert dict(declaration.columns) == {"Customer_ID": "ID"}
    [constraint] = orders.to_desired_table().foreign_keys
    assert tuple(str(c) for c in constraint.local_columns) == ("customer_id",)
    assert tuple(str(c) for c in constraint.referenced_columns) == ("id",)


def test_foreign_key_defensively_copies_mutable_column_input() -> None:
    # Given mutable sequence and mapping declarations
    sequence = ["customer_id"]
    mapping = {"customer_id": "id"}

    # When declarations are constructed and the inputs later change
    sequence_declaration = ForeignKey(columns=sequence, references=_customers())
    mapping_declaration = ForeignKey(columns=mapping, references=_customers())
    sequence.append("tenant_id")
    mapping["tenant_id"] = "tenant_id"

    # Then each declaration retains the input snapshot from construction
    assert sequence_declaration.columns == ("customer_id",)
    assert dict(mapping_declaration.columns) == {"customer_id": "id"}


def test_mapping_insertion_order_is_irrelevant():
    # Given the same composite mapping declared in two insertion orders
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

    # When lowering both declarations
    one = orders_with({"tenant_id": "tenant_id", "customer_id": "id"})
    two = orders_with({"customer_id": "id", "tenant_id": "tenant_id"})

    # Then the constraints are identical, including the generated name
    assert one == two
    assert one.requested_name == two.requested_name


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


def test_mapping_to_an_unknown_parent_column_is_rejected_as_a_key_mismatch():
    # Given a mapping naming a column the parent does not have
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )

    # Then the declaration fails naming the unknown column
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("account_id", Integer())],
            foreign_keys=[
                ForeignKey(
                    columns={"account_id": "unknown_id"},
                    references=accounts,
                )
            ],
        )


def test_two_locals_mapped_to_the_same_key_column_are_rejected():
    with pytest.raises(ValueError):
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

    # When lowering the child against both parent key orders
    before = child_of(["tenant_id", "id"])
    after = child_of(["id", "tenant_id"])

    # Then the lowered constraints are identical
    assert before.foreign_keys == after.foreign_keys

    from delta_engine.domain.model import ObservedForeignKey, ObservedTable
    from tests.builders import as_observed_columns

    # And the reorder produces no drift against the original observed state
    observed = ObservedTable(
        qualified_name=before.qualified_name,
        columns=as_observed_columns(before.columns),
        foreign_keys=tuple(
            ObservedForeignKey(
                local_columns=foreign_key.local_columns,
                referenced_table=foreign_key.referenced_table,
                referenced_columns=foreign_key.referenced_columns,
                catalog_name=foreign_key.requested_name,
            )
            for foreign_key in before.foreign_keys
        ),
    )
    diff = diff_table(after, observed)
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.unresolvable == ()


def test_generated_foreign_key_name_is_identical_across_declaration_casing():
    def declare(local_spelling: str) -> DeltaTable:
        parent = DeltaTable(
            catalog="main",
            schema="sales",
            name="customers",
            columns=[Column("id", Integer(), nullable=False)],
            primary_key=["id"],
        )
        return DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column(local_spelling, Integer())],
            foreign_keys=[ForeignKey(columns={local_spelling: "id"}, references=parent)],
        )

    # When lowering the same declaration in two spellings
    camel = declare("CustomerId").to_desired_table().foreign_keys[0].requested_name
    lower = declare("customerid").to_desired_table().foreign_keys[0].requested_name

    # Then the generated constraint name is identical and lowercase
    assert camel == lower == "orders_customerid_fk"


def test_layout_and_key_references_resolve_across_casing():
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("region", String()), Column("order_id", Integer(), nullable=False)],
        clustered_by=["REGION"],
        primary_key=["ORDER_ID"],
    )

    desired = table.to_desired_table()
    assert desired.primary_key is not None
    assert tuple(str(column) for column in desired.primary_key.columns) == ("order_id",)
    assert tuple(str(column) for column in desired.clustered_by) == ("region",)
