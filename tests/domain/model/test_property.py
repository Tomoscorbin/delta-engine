import pytest

from delta_engine.domain.model.property import DeclaredProperties, TableProperty


def test_table_property_is_the_expected_public_vocabulary() -> None:
    # Then the managed-key vocabulary is exactly the documented set — adding a
    # key is a breaking change (tables carrying it undeclared start failing),
    # so growth must arrive here deliberately
    assert set(TableProperty) == {
        "delta.enableChangeDataFeed",
        "delta.deletedFileRetentionDuration",
        "delta.logRetentionDuration",
        "delta.dataSkippingNumIndexedCols",
        "delta.columnMapping.mode",
        "delta.enableTypeWidening",
    }


def test_declared_properties_reads_as_the_declared_mapping() -> None:
    # Given a declaration setting one key and asserting another absent
    properties = DeclaredProperties(
        {
            TableProperty.CHANGE_DATA_FEED: "true",
            TableProperty.LOG_RETENTION_DURATION: None,
        }
    )

    # Then the declaration reads back as a mapping, absence assertion included
    assert properties == {
        TableProperty.CHANGE_DATA_FEED: "true",
        TableProperty.LOG_RETENTION_DURATION: None,
    }


@pytest.mark.parametrize(
    ("properties", "enabled"),
    [
        ({TableProperty.COLUMN_MAPPING_MODE: "name"}, True),
        ({TableProperty.COLUMN_MAPPING_MODE: "none"}, False),
        ({TableProperty.COLUMN_MAPPING_MODE: None}, False),
        ({}, False),
    ],
)
def test_column_mapping_is_enabled_only_by_mode_name(
    properties: dict[str, str | None],
    enabled: bool,
) -> None:
    # Then only an explicit mode of 'name' counts as column mapping being on
    assert DeclaredProperties(properties).enables_column_mapping() is enabled


@pytest.mark.parametrize(
    ("properties", "enabled"),
    [
        ({TableProperty.CHANGE_DATA_FEED: "true"}, True),
        ({TableProperty.CHANGE_DATA_FEED: "false"}, False),
        ({TableProperty.CHANGE_DATA_FEED: None}, False),
        ({}, False),
    ],
)
def test_change_data_feed_is_enabled_only_by_true(
    properties: dict[str, str | None],
    enabled: bool,
) -> None:
    # Then only an explicit 'true' counts as change data feed being on
    assert DeclaredProperties(properties).enables_change_data_feed() is enabled


@pytest.mark.parametrize(
    ("properties", "enabled"),
    [
        ({TableProperty.TYPE_WIDENING: "true"}, True),
        ({TableProperty.TYPE_WIDENING: "false"}, False),
        ({TableProperty.TYPE_WIDENING: None}, False),
        ({}, False),
    ],
)
def test_type_widening_is_enabled_only_by_true(
    properties: dict[str, str | None],
    enabled: bool,
) -> None:
    # Then only an explicit 'true' counts as type widening being on
    assert DeclaredProperties(properties).enables_type_widening() is enabled
