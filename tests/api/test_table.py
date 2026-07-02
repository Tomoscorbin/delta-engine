import pytest

from delta_engine.api import Column, DeltaTable, ForeignKey, Integer, String
from delta_engine.api.properties import Property
from delta_engine.domain.model import Column as DomainColumn
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint


def test_user_overrides_take_precedence_over_defaults():
    # Given the sole default property (column mapping=name) is overridden by the user
    user_properties = {
        Property.COLUMN_MAPPING_MODE.value: "none",  # user wants to disable
    }
    table = DeltaTable(
        catalog="coredev",
        schema="medallia",
        name="responses",
        columns=[Column("id", Integer())],
        properties=user_properties,
    )

    # When computing effective properties
    effective = table.effective_properties

    # Then the user value wins over the default
    assert effective[Property.COLUMN_MAPPING_MODE.value] == "none"


def test_defaults_are_applied_when_no_user_properties_given():
    # Given a table with no explicit properties
    table = DeltaTable(
        catalog="coredev",
        schema="medallia",
        name="responses",
        columns=[Column("id", Integer())],
    )

    # When computing effective properties
    effective = table.effective_properties

    # Then the column-mapping default is present and no other property is defaulted in
    # (deletion vectors is left to the Databricks runtime default, not managed here)
    assert effective == {Property.COLUMN_MAPPING_MODE.value: "name"}


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
        Property.ENABLE_DELETION_VECTORS.value: "false",
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
    assert table.effective_properties[Property.ENABLE_DELETION_VECTORS.value] == "false"
    assert table.effective_properties[Property.COLUMN_MAPPING_MODE.value] == "name"


def test_accepts_property_enum_members_as_keys():
    # Given properties keyed by the Property enum members directly (not their .value)
    user_properties = {Property.ENABLE_DELETION_VECTORS: "false"}

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
    assert desired.properties[Property.ENABLE_DELETION_VECTORS.value] == "false"
    assert desired.properties[Property.COLUMN_MAPPING_MODE.value] == "name"


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


def test_to_desired_table_carries_effective_properties_with_defaults():
    # Given a table where the user declares a property alongside the defaults
    table = DeltaTable(
        catalog="cat",
        schema="core",
        name="dim_date",
        columns=[Column("id", Integer())],
        properties={Property.ENABLE_DELETION_VECTORS.value: "false"},
    )

    # When converting to the domain table
    desired = table.to_desired_table()

    # Then effective properties (defaults overlaid by user values) are carried through
    assert desired.properties[Property.ENABLE_DELETION_VECTORS.value] == "false"
    assert desired.properties[Property.COLUMN_MAPPING_MODE.value] == "name"


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
    # Given a FK referencing another table
    fk = ForeignKey(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )

    # When constructing the table
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[fk],
    )

    # Then the FK is accessible, carrying its engine-generated constraint name
    assert table.foreign_keys == (fk.with_generated_name("orders"),)


def test_delta_table_defaults_to_no_foreign_keys():
    # Given a table with no foreign_keys argument
    table = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer())],
    )

    # Then
    assert table.foreign_keys == ()


def test_delta_table_rejects_fk_with_unknown_local_column():
    # Given a FK whose local column is not declared in the table
    fk = ForeignKey(
        local_columns=("nonexistent",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )

    # When / Then — domain validation fires at construction time
    with pytest.raises(ValueError, match="nonexistent"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("id", Integer())],
            foreign_keys=[fk],
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
