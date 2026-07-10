import pytest

from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY, Property


def test_property_enum_values_match_the_registry_keys():
    # Given the property vocabulary a user declares against — the enum is the
    # single source, and the catalogue the engine validates against is built
    # from it, so the registry covers exactly the enum's keys
    assert {member.value for member in Property} == set(DELTA_PROPERTY_REGISTRY)

    # And Property is a str enum, so its members can be used directly as dict keys
    assert Property.COLUMN_MAPPING_MODE == "delta.columnMapping.mode"


def test_registry_covers_exactly_the_managed_keys():
    # Given deletion vectors is deliberately absent — Databricks manages it
    assert set(DELTA_PROPERTY_REGISTRY) == {
        "delta.enableChangeDataFeed",
        "delta.deletedFileRetentionDuration",
        "delta.logRetentionDuration",
        "delta.dataSkippingNumIndexedCols",
        "delta.columnMapping.mode",
        "delta.enableTypeWidening",
    }


def test_column_mapping_mode_permits_only_the_upgrade_transition():
    definition = DELTA_PROPERTY_REGISTRY["delta.columnMapping.mode"]

    # The protocol upgrade is one-way
    assert definition.permits_transition("none", "name")
    assert not definition.permits_transition("name", "none")


def test_column_mapping_mode_permits_no_removal():
    # Given the protocol upgrade is permanent — removal is a transition to
    # absence, and no absence transition is permitted
    definition = DELTA_PROPERTY_REGISTRY["delta.columnMapping.mode"]

    assert not definition.permits_transition("name", None)
    assert not definition.permits_transition("none", None)


def test_first_write_is_always_permitted():
    # Given a key absent from the catalog, any declared value may be written,
    # even for the most restricted key
    definition = DELTA_PROPERTY_REGISTRY["delta.columnMapping.mode"]

    assert definition.permits_transition(None, "name")


def test_every_other_key_permits_any_transition_and_removal():
    # Given the pure-configuration keys
    for key, definition in DELTA_PROPERTY_REGISTRY.items():
        if key == "delta.columnMapping.mode":
            continue
        assert definition.permits_transition("anything", "else"), key
        assert definition.permits_transition("anything", None), key


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
        (Property.TYPE_WIDENING, "true"),
        (Property.TYPE_WIDENING, "false"),
    ],
)
def test_registry_accepts_valid_property_values(key: str, value: str) -> None:
    assert DELTA_PROPERTY_REGISTRY[key].reject_declared_value(value) is None


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
        (Property.TYPE_WIDENING, "True"),  # catalog stores lowercase; drift churn otherwise
        (Property.TYPE_WIDENING, "enabled"),
    ],
)
def test_registry_rejects_invalid_property_values(key: str, value: str) -> None:
    rejection = DELTA_PROPERTY_REGISTRY[key].reject_declared_value(value)

    # The message names the key and the expected format
    assert rejection is not None
    assert str(key) in rejection
    assert "Expected" in rejection


def test_declared_none_is_never_rejected():
    # Given None asserts a key's absence, not a value
    for definition in DELTA_PROPERTY_REGISTRY.values():
        assert definition.reject_declared_value(None) is None
