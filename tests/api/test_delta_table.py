from types import MappingProxyType

import pytest

from delta_engine.domain.model import (
    DesiredColumn as DomainColumn,
    ObservedForeignKeyConstraint,
    ObservedTable,
    QualifiedName,
    TableAspect,
    TableScope,
)
from delta_engine.domain.plan.diff import TableDrift, diff_table
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
from tests.builders import as_observed_columns


def _customers() -> DeltaTable:
    """Build a minimal referenced table with a single-column primary key."""
    return DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
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
    # Given a declaration whose name parts and column name use mixed case
    table = DeltaTable(
        catalog="Main",
        schema="Sales",
        name="Orders",
        columns=[Column("Id", Integer())],
    )

    # Then object names are lowercased (Unity Catalog stores them lowercase,
    # live-pinned) while column spelling is catalog display state, preserved
    assert (table.catalog, table.schema, table.name) == ("main", "sales", "orders")
    assert [str(column.name) for column in table.columns] == ["Id"]


def test_delta_table_rejects_columns_differing_only_by_case_as_duplicates() -> None:
    # Given two spellings of one column — the public API delegates to the
    # same identity-keyed validation as the domain layer
    # Then construction fails
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
    # Given a name part Unity Catalog forbids: over 255 characters, or with a
    # period, space, forward slash, ASCII control character, or DEL
    parts = {"catalog": "main", "schema": "sales", "name": "orders", position: bad_part}

    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog=parts["catalog"],
            schema=parts["schema"],
            name=parts["name"],
            columns=[Column("id", Integer())],
        )


def test_column_names_are_exempt_from_object_name_rules():
    # Given a column name containing a forward slash — column-name characters
    # are governed by column mapping, not the Unity Catalog object-name rules
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer()), Column("net/gross", String())],
    )

    # Then the declaration accepts it unchanged
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
    # Given a table declared without clustering
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
    )

    # Then clustered_by is a stable empty tuple, never None
    assert table.clustered_by == ()


def test_delta_table_rejects_partitioning_and_clustering_together():
    # Given a declaration carrying both partitioning and clustering
    # Then construction fails: a table has one physical layout
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
    # Given five clustering keys, one over the Delta limit
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="main",
            schema="sales",
            name="orders",
            columns=[Column(name, Integer()) for name in ("a", "b", "c", "d", "e")],
            clustered_by=["a", "b", "c", "d", "e"],
        )


def test_delta_table_rejects_complex_typed_clustering_column():
    # Given a clustering key of a complex (Map) type
    # Then construction fails
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
    # Given a layout key spelled in a different case from its complex-typed
    # column — declarations are canonical lowercase, so layout identifiers
    # must use the same representation before API policy resolves them
    # Then the complex type is still found and construction fails
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
    # Given a clustering key whose type liquid clustering excludes — Boolean
    # and Binary are valid partition columns but not clustering keys
    # Then construction fails
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
    # Given a partition column of a type clustering would reject
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer()), Column("flag", data_type)],
        partitioned_by=["flag"],
    )

    # Then partitioning accepts it: the two layouts have distinct type rules
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

    # Then construction fails
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
    # Given a partition column of a complex (Array) type
    # Then construction fails
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
    # Given a partition spec covering every column, leaving no data columns
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer()), Column("day", String())],
            partitioned_by=["id", "day"],
        )


def test_partition_keys_are_normalized_before_whole_table_validation() -> None:
    # Given partition keys spelled in a different case from their columns
    # Then normalization resolves them and whole-table validation still
    # rejects partitioning by every column
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

    # Then omission remains explicit so Databricks can choose the physical name
    assert desired.primary_key is not None
    assert desired.primary_key.columns == ("id", "tenant_id")
    assert desired.primary_key.name is None
    assert table.primary_key_name is None


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
    assert desired.primary_key.name == "Accounts_Business_Key"
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
    # Given a physical key name supplied without a primary key
    # Then construction fails
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

    # Then the invalid physical name is rejected at construction
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
    # Given a primary key naming a column the table does not declare
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="events",
            columns=[Column("id", Integer(), nullable=False)],
            primary_key=["missing_col"],
        )


def test_primary_key_over_nullable_column_is_rejected():
    # Given a primary key over a nullable column
    # Then construction fails
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


def test_delta_table_pk_columns_are_canonically_ordered():
    # Given two PK columns declared in non-canonical order
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

    # Then primary_key reports the canonical (sorted) order, not declaration order
    assert table.primary_key == ("order_id", "tenant_id")


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

    # Then the FK definition is lowered without inventing a physical name
    [foreign_key] = table.to_desired_table().foreign_keys
    assert foreign_key.local_columns == ("customer_id",)
    assert foreign_key.referenced_table == QualifiedName("cat", "sch", "customers")
    assert foreign_key.referenced_columns == ("id",)
    assert foreign_key.name is None


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
    assert str(constraint.name) == "Orders_Customer_Relationship"


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
    # Then the invalid physical name is rejected at construction
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
        pytest.param(
            {"customer_id": "id", "CUSTOMER_ID": "region_id"},
            ValueError,
            id="case-duplicate-mapping-keys",
        ),
        pytest.param(
            ["customer_id", "customer_id"],
            ValueError,
            id="duplicate-sequence-entries",
        ),
        pytest.param(
            ["customer_id", "CUSTOMER_ID"],
            ValueError,
            id="case-duplicate-sequence-entries",
        ),
    ],
)
def test_foreign_key_rejects_invalid_column_input(
    columns: object,
    expected_error: type[Exception],
) -> None:
    # When the column input is invalid
    # Then ForeignKey rejects it without needing an owner
    with pytest.raises(expected_error):
        ForeignKey(
            columns=columns,  # type: ignore[arg-type]
            references=_customers(),
        )


@pytest.mark.parametrize(
    ("columns", "references"),
    [
        pytest.param("customer_id", Self, id="single-column"),
        pytest.param(["customer_id", "region_id"], Self, id="column-sequence"),
        pytest.param({"customer_id": "id"}, "cat.sch.customers", id="column-mapping"),
    ],
)
def test_equal_foreign_keys_hash_equal(columns: object, references: object) -> None:
    # Given two declarations built from the same input
    first = ForeignKey(columns=columns, references=references)  # type: ignore[arg-type]
    second = ForeignKey(columns=columns, references=references)  # type: ignore[arg-type]

    # Then they are equal and hash equal
    assert first == second
    assert hash(first) == hash(second)


def test_reordered_mapping_foreign_keys_collapse_in_sets() -> None:
    # Given the same composite mapping declared in two insertion orders
    first = ForeignKey(
        columns={"tenant_id": "tenant_id", "customer_id": "id"},
        references="cat.sch.accounts",
    )
    second = ForeignKey(
        columns={"customer_id": "id", "tenant_id": "tenant_id"},
        references="cat.sch.accounts",
    )

    # Then the declarations are interchangeable in hashed collections
    assert len({first, second}) == 1


@pytest.mark.parametrize(
    ("first_columns", "second_columns", "references"),
    [
        pytest.param("Customer_ID", "customer_id", Self, id="single-column"),
        pytest.param(
            ["Customer_ID", "Region_ID"],
            ["customer_id", "region_id"],
            Self,
            id="column-sequence",
        ),
        pytest.param(
            {"Customer_ID": "ID"},
            {"customer_id": "id"},
            "cat.sch.customers",
            id="column-mapping",
        ),
    ],
)
def test_foreign_keys_differing_only_in_column_case_are_equal(
    first_columns: object,
    second_columns: object,
    references: object,
) -> None:
    # Given two declarations spelling the same columns in different cases
    first = ForeignKey(columns=first_columns, references=references)  # type: ignore[arg-type]
    second = ForeignKey(columns=second_columns, references=references)  # type: ignore[arg-type]

    # Then they are equal and hash equal
    assert first == second
    assert hash(first) == hash(second)


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

    # Then the public declarations round-trip without acquiring an owner-derived name
    assert copy.foreign_keys == original.foreign_keys
    [constraint] = copy.to_desired_table().foreign_keys
    assert constraint.name is None


def test_delta_table_rejects_fk_with_unknown_local_column():
    # Given a referenced table and a FK whose local column is not declared
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )

    # Then constructing the owner fails: it does not declare the local column
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


def test_delta_table_accepts_free_form_tag_keys():
    # Given an arbitrary tag key (tags are free-form, unlike the Property
    # allowlist), including an interior space — only leading and trailing
    # spaces are forbidden
    table = DeltaTable(
        catalog="cat",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer())],
        tags={"any custom_key": "v"},
    )

    # Then construction succeeds and the key is preserved (no ValueError)
    assert dict(table.to_desired_table().tags) == {"any custom_key": "v"}


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
    # Then construction fails
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


def test_rejects_unmanaged_key_declared_as_none():
    # Given an absence assertion for a key the engine does not manage
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="coredev",
            schema="medallia",
            name="responses",
            columns=[Column("id", Integer())],
            properties={"delta.enableRowTracking": None},
        )


def test_delta_table_rejects_invalid_property_value() -> None:
    # Given a boolean property carrying a value Delta does not accept
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer())],
            properties={"delta.enableChangeDataFeed": "yes"},
        )


def test_delta_table_accepts_none_property_value_without_value_check() -> None:
    # Given a declaration asserting a key absent — None asserts absence; it is
    # not a value and must not be validated as one
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        properties={"delta.enableChangeDataFeed": None},
    )

    # Then the absence assertion is carried through to the desired table
    assert table.to_desired_table().properties["delta.enableChangeDataFeed"] is None


@pytest.mark.parametrize("scope", _SCOPES_WITHOUT_COLUMN_STRUCTURE)
def test_a_restricted_scope_mirrors_cdf_reserved_column_names(scope) -> None:
    # Given a restricted-scope declaration naming a CDF reserved column — a
    # declaration that does not manage column structure never creates or adds
    # a column, so naming one mirrors state the catalog already holds; it
    # carries the CDF property for the same reason, not to enable the feature
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer()), Column("_change_type", String())],
        properties={"delta.enableChangeDataFeed": "true"},
        scope=scope,
    )

    # Then the mirrored column is accepted
    assert len(table.to_desired_table().columns) == 2


def test_delta_table_accepts_column_tags_at_the_limits() -> None:
    # Given a column carrying 50 tags of 256 characters — a column is its own
    # securable, so the limits apply per column
    at_limit = {f"tag_{i}": "x" * 256 for i in range(50)}
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer(), tags=at_limit)],
    )

    # Then the declaration accepts all of them
    assert len(table.to_desired_table().columns[0].tags) == 50


def test_delta_table_rejects_special_character_column_names_without_column_mapping() -> None:
    # Given a column name with a space but no column-mapping property
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("order id", Integer())],
        )


def test_delta_table_accepts_special_character_column_names_with_column_mapping() -> None:
    # Given the same column name with column mapping declared
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("order id", Integer())],
        properties={"delta.columnMapping.mode": "name"},
    )

    # Then the declaration accepts it unchanged
    assert table.to_desired_table().columns[0].name == "order id"


def test_delta_table_rejects_special_character_struct_field_names_without_column_mapping() -> None:
    # Given a struct field name with a space but no column-mapping property
    # Then construction fails
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
    # Given the same struct field name with column mapping declared
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[
            Column("payload", Struct((StructField("order id", Integer()),))),
        ],
        properties={"delta.columnMapping.mode": "name"},
    )

    # Then the field survives lowering unchanged
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
    # Given a column whose non-nullable struct fields sit under nullable or
    # container parents (invalid) or under a fully non-nullable path (valid)
    column = Column("payload", data_type, nullable=column_nullable)

    # Then invalid placements fail construction and valid ones lower unchanged
    if not is_valid:
        with pytest.raises(ValueError):
            DeltaTable("dev", "silver", "orders", columns=[column])
        return

    table = DeltaTable("dev", "silver", "orders", columns=[column])
    assert table.to_desired_table().columns[0].data_type == data_type


def test_delta_table_rejects_special_character_field_names_in_struct_nested_in_array() -> None:
    # Given a bad field name inside a struct nested in an array — the gate
    # must recurse through container types, not just direct struct columns
    # Then construction fails
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
    # Given a restricted-scope declaration omitting the property that made an
    # observed column name valid
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("order id", Integer())],
        scope=scope,
    )

    # Then the catalog-dependent name check is relaxed
    assert [column.name for column in table.to_desired_table().columns] == ["order id"]

    # Then nullability no Delta schema can retain is still rejected
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
    # Given a CDF reserved column name on a table enabling CDF
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer()), Column("_change_type", String())],
            properties={"delta.enableChangeDataFeed": "true"},
        )


def test_delta_table_accepts_cdf_reserved_column_names_when_cdf_not_enabled() -> None:
    # Given a CDF reserved column name on a table without CDF
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer()), Column("_change_type", String())],
    )

    # Then the name is just a name
    assert len(table.to_desired_table().columns) == 2


# ---- tag limits (Unity Catalog) ----


def test_delta_table_rejects_more_than_fifty_table_tags() -> None:
    # Given 51 table tags, one over the Unity Catalog limit
    too_many = {f"tag_{i}": "v" for i in range(51)}

    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer())],
            tags=too_many,
        )


def test_delta_table_rejects_overlong_tag_value_on_a_column() -> None:
    # Given a column tag value one character over the 256 limit
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer(), tags={"note": "x" * 257})],
        )


def test_delta_table_rejects_overlong_tag_key_on_a_column() -> None:
    # Given a column tag key one character over the 256 limit
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer(), tags={"x" * 257: "note"})],
        )


def test_delta_table_accepts_a_tag_key_at_the_length_limit() -> None:
    # Given a tag key exactly at the 256-character boundary
    at_limit = "k" * 256
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer(), tags={at_limit: "note"})],
    )

    # Then it is accepted: the guard rejects only beyond the limit
    assert at_limit in table.to_desired_table().columns[0].tags


def test_delta_table_accepts_tags_at_the_limits() -> None:
    # Given 50 table tags of 256 characters, both at their limits
    at_limit = {f"tag_{i}": "x" * 256 for i in range(50)}
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        tags=at_limit,
    )

    # Then the declaration accepts all of them
    assert len(table.to_desired_table().tags) == 50


@pytest.mark.parametrize("character", [".", ",", "-", "=", "/", ":"])
def test_delta_table_rejects_a_forbidden_character_in_a_table_tag_key(character: str) -> None:
    # Given a table tag key containing a character Unity Catalog forbids in keys
    key = f"cost{character}centre"

    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer())],
            tags={key: "v"},
        )


def test_delta_table_rejects_a_forbidden_character_in_a_column_tag_key() -> None:
    # Given a column tag key containing a character Unity Catalog forbids in keys
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer(), tags={"pii-class": "high"})],
        )


def test_delta_table_accepts_forbidden_key_characters_in_a_tag_value() -> None:
    # Given a tag value using every character forbidden in keys
    value = "team-data/eng:eu, v=1."
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        tags={"owner": value},
    )

    # Then the value passes through: the character rule binds keys only
    assert table.to_desired_table().tags["owner"] == value


@pytest.mark.parametrize(
    "tags",
    [
        {" env": "prod"},
        {"env ": "prod"},
        {"env": " prod"},
        {"env": "prod "},
    ],
)
def test_delta_table_rejects_a_leading_or_trailing_space_in_a_tag_key_or_value(
    tags: dict[str, str],
) -> None:
    # Given a tag whose key or value begins or ends with a space
    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer())],
            tags=tags,
        )


def test_delta_table_accepts_one_thousand_column_tags_in_total() -> None:
    # Given 20 columns each carrying its 50-tag quota — exactly the 1,000
    # column-tag table total — plus 50 table tags, which do not count
    columns = [
        Column(f"column_{i}", Integer(), tags={f"tag_{j}": "v" for j in range(50)})
        for i in range(20)
    ]
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=columns,
        tags={f"table_tag_{i}": "v" for i in range(50)},
    )

    # Then the declaration is accepted at the boundary
    assert sum(len(column.tags) for column in table.columns) == 1000


def test_delta_table_rejects_more_than_one_thousand_column_tags_in_total() -> None:
    # Given columns whose tags total 1,001 while every column stays within
    # its own 50-tag quota
    columns = [
        Column(f"column_{i}", Integer(), tags={f"tag_{j}": "v" for j in range(50)})
        for i in range(20)
    ]
    columns.append(Column("column_20", Integer(), tags={"tag_0": "v"}))

    # Then construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=columns,
        )


def test_delta_table_rejects_rename_hint_without_column_mapping():
    # Given a rename hint on a table without column mapping
    # Then construction fails: renames need stable physical names
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
    # Given a rename hint on a table with column mapping declared
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

    # Then the hint is preserved on the column
    assert table.columns[1].renamed_from == "customer_nm"


@pytest.mark.parametrize(
    "columns",
    ["Customer_ID", ["Customer_ID"]],
    ids=["bare-string", "sequence"],
)
def test_single_column_shorthand_foreign_key_infers_and_preserves_spelling(columns):
    # Given a parent with a single-column primary key and a shorthand
    # declaration naming only the local column
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("ID", Integer(), nullable=False)],
        primary_key=["ID"],
    )
    declaration = ForeignKey(columns=columns, references=customers)

    # When the owning table is constructed
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("Customer_ID", Integer())],
        foreign_keys=[declaration],
    )

    # Then the referenced key is inferred and both spellings are preserved
    [foreign_key] = orders.to_desired_table().foreign_keys
    assert declaration.columns == ("Customer_ID",)
    assert tuple(str(c) for c in foreign_key.local_columns) == ("Customer_ID",)
    assert tuple(str(c) for c in foreign_key.referenced_columns) == ("ID",)


def test_same_name_composite_foreign_key_is_paired_by_name_not_declared_order():
    # Given a composite key whose local columns share the parent's names,
    # declared in reverse order relative to the parent
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
                columns=["Account_ID", "Tenant_ID"],
                references=accounts,
            )
        ],
    )

    # Then each local column pairs with the parent column of the same name
    [foreign_key] = entries.to_desired_table().foreign_keys
    assert foreign_key.local_columns == ("Account_ID", "Tenant_ID")
    assert foreign_key.referenced_columns == ("Account_ID", "Tenant_ID")


def test_ambiguous_composite_foreign_key_requires_mapping():
    # Given a composite shorthand whose names do not match the parent's key
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

    # Then construction fails: the pairing is ambiguous without a mapping
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


def test_foreign_key_declaration_rejects_internal_constraint_fields():
    # Then supplying lowered domain fields fails construction
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

    # Then constructing the child fails: there is no target key to infer against
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("id", Integer()), Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def test_delta_table_rejects_self_reference_without_primary_key():
    # Given a self-reference on a table without a primary key
    # Then construction fails
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

    # Then declaring the child fails: information_schema is per-catalog, so
    # the engine could create the constraint but never observe it, and every
    # later sync would re-plan and fail
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def _table_with_distinct_foreign_keys(
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


def test_unnamed_foreign_keys_need_no_engine_name_collision_handling():
    # Given two structurally distinct FKs that once produced the same engine name
    orders = _table_with_distinct_foreign_keys()

    # Then both remain unnamed and Databricks owns their schema-unique names
    assert tuple(constraint.name for constraint in orders.to_desired_table().foreign_keys) == (
        None,
        None,
    )


def test_explicit_foreign_key_name_is_independent_of_unnamed_foreign_keys():
    # Given one unnamed FK and one with a custom physical name

    orders = _table_with_distinct_foreign_keys((None, "orders_parts_two_fk"))

    # Then only the explicitly supplied creation preference is retained
    assert tuple(constraint.name for constraint in orders.to_desired_table().foreign_keys) == (
        None,
        "orders_parts_two_fk",
    )


def test_foreign_key_rejects_non_table_reference_during_its_construction():
    # Given a reference that is neither a DeltaTable, Self, nor a name string
    # Then construction fails
    with pytest.raises(TypeError):
        ForeignKey(
            columns={"customer_id": "id"},
            references=123,  # type: ignore[arg-type]
        )


# ---------- name references ----------


def test_name_reference_lowers_to_the_named_table():
    # Given a foreign key referencing its parent by full name, with no
    # parent object anywhere in scope
    declaration = ForeignKey(columns={"customer_id": "id"}, references="cat.sch.customers")

    # When the owning table is constructed in that same catalog
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("customer_id", Integer())],
        foreign_keys=[declaration],
    )

    # Then the reference lowers to the named table with the mapped columns
    [foreign_key] = orders.to_desired_table().foreign_keys
    assert foreign_key.referenced_table == QualifiedName("cat", "sch", "customers")
    assert foreign_key.local_columns == ("customer_id",)
    assert foreign_key.referenced_columns == ("id",)


@pytest.mark.parametrize("reference", ["customers", "sch.customers", "cat.sch.customers.extra"])
def test_name_reference_rejects_anything_but_a_three_part_name(reference: str):
    # Given a reference that is not exactly catalog.schema.table
    # When it is used in a foreign key
    # Then construction fails
    with pytest.raises(ValueError):
        ForeignKey(columns={"customer_id": "id"}, references=reference)


def test_name_reference_rejects_a_foreign_catalog():
    # Given a foreign key naming a table in another catalog
    declaration = ForeignKey(columns={"customer_id": "id"}, references="other.sch.customers")

    # When the owning table is constructed, then the cross-catalog
    # relationship is rejected exactly as it is for object references
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("customer_id", Integer())],
            foreign_keys=[declaration],
        )


@pytest.mark.parametrize("columns", ["customer_id", ["customer_id"], ("customer_id",)])
def test_name_reference_requires_an_explicit_column_mapping(
    columns: str | list[str] | tuple[str, ...],
):
    # Given a name reference combined with a shorthand column form
    # Then construction fails: shorthands resolve against the parent's
    # primary key, which a name does not carry
    with pytest.raises(ValueError):
        ForeignKey(columns=columns, references="cat.sch.customers")


@pytest.mark.parametrize("reference", [".silver.events", "cat..events", "cat.silver."])
def test_name_reference_rejects_a_blank_part(reference: str):
    # Given a dotted name with one blank part
    # When it is used in a foreign key
    # Then construction fails
    with pytest.raises(ValueError):
        ForeignKey(columns={"customer_id": "id"}, references=reference)


def test_name_reference_rejects_forbidden_characters():
    # Given a table name with a character Unity Catalog forbids in object names
    reference = "cat.sch.my table"

    # When it is used in a foreign key
    # Then construction fails, rather than waiting for DDL to be rejected
    with pytest.raises(ValueError):
        ForeignKey(columns={"customer_id": "id"}, references=reference)


def test_name_reference_rejects_an_over_long_part():
    # Given a table name one character over Unity Catalog's object-name limit
    over_long = "t" * 256

    # Then using it in a name reference fails construction
    with pytest.raises(ValueError):
        ForeignKey(columns={"customer_id": "id"}, references=f"cat.sch.{over_long}")


def test_foreign_key_rejects_local_column_type_mismatch_with_target_primary_key():
    # Given a referenced primary key of type Long and a local column of type String
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Long(), nullable=False)],
        primary_key=["id"],
    )

    # Then constructing the child fails on the type mismatch
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
    # Given a self-reference whose local column type differs from the key's
    # Then construction fails
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


def test_self_referential_foreign_key_with_nonexistent_primary_key_column_is_rejected() -> None:
    # Given a primary key naming a column the table does not declare
    columns = [
        Column("id", Integer(), nullable=False),
        Column("manager_id", Integer()),
    ]

    # When the table also declares a self-referential foreign key against that key
    # Then the declaration is rejected with a ValueError, not an internal error
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="employees",
            columns=columns,
            primary_key=["ghost"],
            foreign_keys=[ForeignKey(columns="manager_id", references=Self)],
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

    # Then constructing the child fails on the incompatible pair
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
    assert {str(local): str(referenced) for local, referenced in declaration.columns.items()} == {
        "Customer_ID": "ID"
    }
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

    # Then the structurally identical constraints compare equal
    assert one == two
    assert one.name == two.name


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
    # Given two local columns mapped to the same parent key column
    # Then construction fails
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

    # And the reorder produces no drift against the original observed state
    observed = ObservedTable(
        qualified_name=before.qualified_name,
        columns=as_observed_columns(before.columns),
        foreign_keys=(
            ObservedForeignKeyConstraint(
                local_columns=before.foreign_keys[0].local_columns,
                referenced_table=before.foreign_keys[0].referenced_table,
                referenced_columns=before.foreign_keys[0].referenced_columns,
                name="databricks_generated_fk",
            ),
        ),
    )
    diff = diff_table(after, observed)
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.unresolvable == ()


def test_layout_and_key_references_resolve_across_casing():
    # Given layout and key references spelled in a different case
    table = DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("region", String()), Column("order_id", Integer(), nullable=False)],
        clustered_by=["REGION"],
        primary_key=["ORDER_ID"],
    )

    # Then they resolve to the columns' canonical spelling
    desired = table.to_desired_table()
    assert desired.primary_key is not None
    assert tuple(str(column) for column in desired.primary_key.columns) == ("order_id",)
    assert tuple(str(column) for column in desired.clustered_by) == ("region",)


# ---------- immutability of a constructed declaration ----------


def _orders_table() -> DeltaTable:
    return DeltaTable(
        catalog="main",
        schema="sales",
        name="orders",
        columns=[Column("id", Integer(), nullable=False)],
    )


def test_declared_state_cannot_be_replaced_after_construction():
    # Given a constructed declaration
    table = _orders_table()

    # When code tries to swap in a different backing table
    # Then the immutability guard refuses it and the original state is unchanged
    with pytest.raises(AttributeError):
        table._desired_table = _orders_table().to_desired_table()
    assert table.name == "orders"


def test_foreign_keys_cannot_be_replaced_after_construction():
    # Given a constructed declaration
    table = _orders_table()

    # When code tries to overwrite its foreign-key declarations
    # Then the immutability guard refuses it and the declaration keeps no foreign keys
    with pytest.raises(AttributeError):
        table._foreign_key_declarations = ("injected",)
    assert table.foreign_keys == ()


def test_arbitrary_attributes_cannot_be_added_to_a_declaration():
    # Given a constructed declaration
    table = _orders_table()

    # When code tries to attach a new attribute
    # Then the immutability guard refuses it
    with pytest.raises(AttributeError):
        table.owner = "someone-else"


def test_declared_attributes_cannot_be_deleted():
    # Given a constructed declaration
    table = _orders_table()

    # When code tries to delete its backing state
    # Then the immutability guard refuses it and the declaration stays usable
    with pytest.raises(AttributeError):
        del table._desired_table
    assert table.to_desired_table().qualified_name.name == "orders"
