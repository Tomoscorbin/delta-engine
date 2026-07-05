from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY


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
