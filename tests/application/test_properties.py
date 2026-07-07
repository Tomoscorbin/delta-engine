import pytest

from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY, Property


def test_property_enum_values_match_the_registry_keys():
    # Given the property vocabulary a user declares against — the enum is the
    # single source, and the catalogue the engine validates against is built
    # from it, so the registry covers exactly the enum's keys
    assert {member.value for member in Property} == set(DELTA_PROPERTY_REGISTRY)

    # And Property is a str enum, so its members can be used directly as dict keys
    assert Property.COLUMN_MAPPING_MODE == "delta.columnMapping.mode"


def test_registry_covers_the_five_managed_keys():
    # Given deletion vectors is deliberately absent — Databricks manages it
    assert set(DELTA_PROPERTY_REGISTRY) == {
        "delta.enableChangeDataFeed",
        "delta.deletedFileRetentionDuration",
        "delta.logRetentionDuration",
        "delta.dataSkippingNumIndexedCols",
        "delta.columnMapping.mode",
    }


def test_column_mapping_mode_permits_only_the_upgrade_transition():
    definition = DELTA_PROPERTY_REGISTRY["delta.columnMapping.mode"]

    assert definition.permitted_transitions == frozenset({("none", "name")})


def test_column_mapping_mode_permits_no_removal():
    # Given the protocol upgrade is permanent — removal is a transition to
    # absence, and no (value, None) pair is permitted
    transitions = DELTA_PROPERTY_REGISTRY["delta.columnMapping.mode"].permitted_transitions

    assert not any(desired is None for _, desired in transitions)


def test_every_other_key_is_unrestricted():
    # Given the four pure-configuration keys
    unrestricted = {
        key
        for key, definition in DELTA_PROPERTY_REGISTRY.items()
        if definition.permitted_transitions == frozenset()
    }

    assert unrestricted == set(DELTA_PROPERTY_REGISTRY) - {"delta.columnMapping.mode"}


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (Property.CHANGE_DATA_FEED, "true"),
        (Property.CHANGE_DATA_FEED, "false"),
        (Property.COLUMN_MAPPING_MODE, "name"),
        (Property.COLUMN_MAPPING_MODE, "none"),
        (Property.LOG_RETENTION_DURATION, "interval 30 days"),
        (Property.LOG_RETENTION_DURATION, "INTERVAL 1 WEEK"),
        (Property.DELETED_FILE_RETENTION_DURATION, "interval 7 days"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "-1"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "32"),
    ],
)
def test_registry_accepts_valid_property_values(key: str, value: str) -> None:
    assert DELTA_PROPERTY_REGISTRY[key].is_valid_value(value)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (Property.CHANGE_DATA_FEED, "True"),  # catalog stores lowercase; drift churn otherwise
        (Property.CHANGE_DATA_FEED, "yes"),
        (Property.COLUMN_MAPPING_MODE, "id"),
        (Property.LOG_RETENTION_DURATION, "30 days"),
        (Property.LOG_RETENTION_DURATION, "interval thirty days"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "-2"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "many"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "1_000"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "+5"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, " 5 "),
    ],
)
def test_registry_rejects_invalid_property_values(key: str, value: str) -> None:
    assert not DELTA_PROPERTY_REGISTRY[key].is_valid_value(value)
