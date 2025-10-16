import pytest

from delta_engine.adapters.schema import DeltaTable
from delta_engine.adapters.schema.delta.properties import Property


class _Column:
    def __init__(self, name: str):
        self.name = name


def test_user_overrides_take_precedence_over_defaults():
    # Given default properties include deletion vectors=true, column mapping=name
    user_properties = {
        Property.ENABLE_DELETION_VECTORS.value: "false",  # user wants to disable
    }
    table = DeltaTable(
        catalog="CoreDev",
        schema="Medallia",
        name="Responses",
        columns=[_Column("id")],
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
        catalog="CoreDev",
        schema="Medallia",
        name="Responses",
        columns=[_Column("id")],
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
            catalog="CoreDev",
            schema="Medallia",
            name="Responses",
            columns=[_Column("id")],
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
        catalog="CoreDev",
        schema="Medallia",
        name="Responses",
        columns=[_Column("id")],
        properties=user_properties,
    )

    # Then it succeeds and the keys are intact
    assert table.effective_properties[Property.ENABLE_DELETION_VECTORS.value] == "false"
    assert table.effective_properties[Property.COLUMN_MAPPING_MODE.value] == "name"


def test_partition_columns_must_exist_case_insensitive():
    # Given columns include 'event_date', partition spec uses different case
    table = DeltaTable(
        catalog="CoreDev",
        schema="Medallia",
        name="Responses",
        columns=[_Column("id"), _Column("event_date")],
        partitioned_by=["EVENT_DATE"],
    )

    # When table is constructed
    # Then no error because matching is case-insensitive
    assert table.partitioned_by == ["EVENT_DATE"]


def test_missing_partition_column_raises_error():
    # Given partition spec references a column that does not exist
    # When/Then: construction fails
    with pytest.raises(ValueError):
        DeltaTable(
            catalog="CoreDev",
            schema="Medallia",
            name="Responses",
            columns=[_Column("id"), _Column("event_date")],
            partitioned_by=["store_id"],  # not present
        )
