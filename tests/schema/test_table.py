import pytest

from delta_engine.domain.model import Column as DomainColumn
from delta_engine.schema import Column, DeltaTable, Integer, String
from delta_engine.schema.properties import Property


def test_user_overrides_take_precedence_over_defaults():
    # Given default properties include deletion vectors=true, column mapping=name
    user_properties = {
        Property.ENABLE_DELETION_VECTORS.value: "false",  # user wants to disable
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

    # Then user value wins; other defaults still present
    assert effective[Property.ENABLE_DELETION_VECTORS.value] == "false"
    assert effective[Property.COLUMN_MAPPING_MODE.value] == "name"


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

    # Then all defaults are present
    assert effective[Property.ENABLE_DELETION_VECTORS.value] == "true"
    assert effective[Property.COLUMN_MAPPING_MODE.value] == "name"


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
    table = DeltaTable(
        catalog="coredev",
        schema="medallia",
        name="responses",
        columns=[Column("id", Integer()), Column("event_date", String())],
        partitioned_by=["store_id"],  # not present
    )

    # When/Then converting to the domain table fails on the domain invariant
    with pytest.raises(ValueError):
        table.to_desired_table()


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
    # Given a table where the user overrides one default property
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
    # Given a table with no partition spec
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
