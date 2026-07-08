import pytest

from delta_engine.api.table import METADATA_ASPECTS
from delta_engine.domain.model import (
    ALL_ASPECTS,
    Column as DomainColumn,
    QualifiedName,
    TableAspect,
)
from delta_engine.domain.model.constraints import PrimaryKeyConstraint
from delta_engine.schema import (
    Array,
    Column,
    DeltaTable,
    ForeignKey,
    Integer,
    Property,
    String,
    Struct,
    StructField,
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


def test_column_with_primary_key_flag():
    # Given a Column with primary_key=True
    col = Column("id", Integer(), nullable=False, primary_key=True)

    # Then the flag is readable
    assert col.primary_key is True


def test_column_primary_key_defaults_to_false():
    # Given a Column without the primary_key flag
    col = Column("id", Integer())

    # Then it defaults to False
    assert col.primary_key is False


def test_delta_table_primary_key_returns_pk_column_names():
    # Given a DeltaTable with one PK column
    table = DeltaTable(
        catalog="c",
        schema="s",
        name="orders",
        columns=[
            Column("id", Integer(), nullable=False, primary_key=True),
            Column("name", String()),
        ],
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
            Column("id", Integer(), nullable=False, primary_key=True),
            Column("ds", String()),
        ],
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
            Column("tenant_id", Integer(), nullable=False, primary_key=True),
            Column("order_id", Integer(), nullable=False, primary_key=True),
            Column("ds", String()),
        ],
    )

    # Then the order in primary_key matches declaration order
    assert table.primary_key == ("tenant_id", "order_id")


def test_delta_table_accepts_foreign_keys_parameter():
    # Given a referenced table with a primary key
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False, primary_key=True)],
    )

    # When constructing a table with a FK to it
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[ForeignKey(local_columns=("customer_id",), references=customers)],
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
        columns=[Column("id", Integer(), nullable=False, primary_key=True)],
    )

    # When / Then domain validation fires at construction time
    with pytest.raises(ValueError, match="nonexistent"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("id", Integer())],
            foreign_keys=[ForeignKey(local_columns=("nonexistent",), references=customers)],
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


# ---- metadata_only flag


def test_delta_table_manages_all_aspects_by_default():
    # Given a table declared without metadata_only
    table = DeltaTable(
        catalog="dev", schema="silver", name="orders", columns=[Column("id", Integer())]
    )

    # Then the lowered desired table manages everything
    assert table.to_desired_table().managed_aspects == ALL_ASPECTS


def test_metadata_only_table_manages_metadata_aspects():
    # Given a metadata-only declaration
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        metadata_only=True,
    )

    # Then the lowered scope is exactly the metadata aspects
    assert table.to_desired_table().managed_aspects == METADATA_ASPECTS


def test_metadata_only_declaration_carries_properties_without_deploying_them():
    # Given a metadata-only declaration of a full table, properties included —
    # the flag scopes deployment, not what may be declared
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        properties={Property.CHANGE_DATA_FEED.value: "true"},
        metadata_only=True,
    )

    # Then the declaration carries the property; PROPERTIES stays unmanaged
    desired = table.to_desired_table()
    assert desired.properties == {Property.CHANGE_DATA_FEED.value: "true"}
    assert TableAspect.PROPERTIES not in desired.managed_aspects


def test_metadata_only_without_properties_constructs_cleanly():
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        metadata_only=True,
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


def test_metadata_only_table_mirrors_cdf_reserved_columns_with_cdf_declared() -> None:
    # metadata_only declarations do not manage properties, so the declaration
    # is a mirror of existing state, not an attempt to enable CDF.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer()), Column("_change_type", String())],
        properties={"delta.enableChangeDataFeed": "true"},
        metadata_only=True,
    )
    assert len(table.to_desired_table().columns) == 2


def test_delta_table_accepts_column_tags_at_the_limits() -> None:
    # A column is its own securable: 50 tags of 1,000 characters are accepted
    at_limit = {f"tag_{i}": "x" * 1000 for i in range(50)}
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer(), tags=at_limit)],
    )
    assert len(table.to_desired_table().columns[0].tags) == 50


def test_metadata_only_table_still_lowers_the_full_schema():
    # Given a metadata-only declaration with full schema detail
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer(), nullable=False), Column("name", String())],
        metadata_only=True,
    )

    # Then all columns are lowered — scope controls reconciliation, not lowering
    desired = table.to_desired_table()
    assert tuple(c.name for c in desired.columns) == ("id", "name")


def test_metadata_aspects_excludes_structure_properties_and_partitioning():
    # Given the metadata-only named mode
    # Then physical-behaviour aspects are excluded by design
    assert METADATA_ASPECTS == ALL_ASPECTS - frozenset(
        {
            TableAspect.COLUMN_STRUCTURE,
            TableAspect.PROPERTIES,
            TableAspect.PARTITIONING,
        }
    )


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


def test_metadata_only_table_mirrors_special_character_columns_without_the_property() -> None:
    # metadata_only never creates or adds columns; the catalog already
    # accepted this name, so the declaration must be able to mirror it.
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("order id", Integer())],
        metadata_only=True,
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
    with pytest.raises(ValueError, match="1000"):
        DeltaTable(
            catalog="dev",
            schema="silver",
            name="orders",
            columns=[Column("id", Integer(), tags={"note": "x" * 1001})],
        )


def test_delta_table_accepts_tags_at_the_limits() -> None:
    at_limit = {f"tag_{i}": "x" * 1000 for i in range(50)}
    table = DeltaTable(
        catalog="dev",
        schema="silver",
        name="orders",
        columns=[Column("id", Integer())],
        tags=at_limit,
    )
    assert len(table.to_desired_table().tags) == 50
