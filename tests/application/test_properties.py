import pytest

from delta_engine.application.properties import DELTA_PROPERTY_POLICY, Property


def test_property_is_the_expected_public_vocabulary() -> None:
    # Then the managed-key vocabulary is exactly the documented set — adding a
    # key is a breaking change (tables carrying it undeclared start failing),
    # so growth must arrive here deliberately
    assert set(Property) == {
        "delta.enableChangeDataFeed",
        "delta.deletedFileRetentionDuration",
        "delta.logRetentionDuration",
        "delta.dataSkippingNumIndexedCols",
        "delta.columnMapping.mode",
        "delta.enableTypeWidening",
    }


def test_policy_rejects_an_unmanaged_declared_property() -> None:
    # When a declaration carries a key the engine does not manage
    # Then validation fails
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.validate_declaration({"delta.enableRowTracking": "true"})


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (Property.CHANGE_DATA_FEED, "true"),
        (Property.CHANGE_DATA_FEED, "false"),
        (Property.COLUMN_MAPPING_MODE, "name"),
        (Property.COLUMN_MAPPING_MODE, "none"),
        (Property.LOG_RETENTION_DURATION, "interval 30 days"),
        (Property.DELETED_FILE_RETENTION_DURATION, "interval 7 days"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "-1"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "32"),
        (Property.TYPE_WIDENING, "true"),
        (Property.TYPE_WIDENING, "false"),
    ],
)
def test_policy_accepts_valid_property_values(key: str, value: str) -> None:
    # Then a canonical value for each managed key is accepted
    DELTA_PROPERTY_POLICY.validate_declaration({key: value})


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (Property.CHANGE_DATA_FEED, "True"),
        (Property.CHANGE_DATA_FEED, "yes"),
        (Property.COLUMN_MAPPING_MODE, "id"),
        (Property.LOG_RETENTION_DURATION, "30 days"),
        (Property.LOG_RETENTION_DURATION, "INTERVAL 1 WEEK"),
        (Property.LOG_RETENTION_DURATION, "interval thirty days"),
        (Property.LOG_RETENTION_DURATION, "interval 1 hour 30 minutes"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "-2"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "many"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "1_000"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, "+5"),
        (Property.DATA_SKIPPING_NUM_INDEXED_COLS, " 5 "),
        (Property.TYPE_WIDENING, "True"),
        (Property.TYPE_WIDENING, "enabled"),
    ],
)
def test_policy_rejects_invalid_property_values(key: str, value: str) -> None:
    # When a declared value is not the catalog's canonical spelling
    # Then validation fails — a non-canonical value would re-diff as drift on
    # every sync, or fail Java-side parsing at execution instead of declaration
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.validate_declaration({key: value})


@pytest.mark.parametrize("key", Property)
def test_policy_accepts_none_as_an_absence_assertion(key: str) -> None:
    # Then a None value — asserting the key's absence — is valid for every key
    DELTA_PROPERTY_POLICY.validate_declaration({key: None})


def test_policy_projects_only_managed_observed_properties() -> None:
    # Given a catalog map mixing managed and platform-owned keys
    observed = DELTA_PROPERTY_POLICY.project_observed(
        {
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "3",
            "delta.feature.columnMapping": "supported",
        }
    )

    # Then only the managed key survives the projection
    assert dict(observed) == {
        "delta.columnMapping.mode": "name",
    }


def test_policy_permits_only_the_column_mapping_upgrade() -> None:
    # Then none -> name is permitted, and the downgrade is not
    assert DELTA_PROPERTY_POLICY.permits_transition(
        "delta.columnMapping.mode",
        observed="none",
        desired="name",
    )
    assert not DELTA_PROPERTY_POLICY.permits_transition(
        "delta.columnMapping.mode",
        observed="name",
        desired="none",
    )


@pytest.mark.parametrize("observed", ["none", "name"])
def test_policy_does_not_permit_column_mapping_removal(observed: str) -> None:
    # Then declaring the key absent is blocked whatever its current value
    assert not DELTA_PROPERTY_POLICY.permits_removal(Property.COLUMN_MAPPING_MODE, observed)


def test_policy_permits_first_write_of_a_restricted_property() -> None:
    # Then a key absent from the catalog may always be written
    assert DELTA_PROPERTY_POLICY.permits_transition(
        Property.COLUMN_MAPPING_MODE,
        observed=None,
        desired="name",
    )


@pytest.mark.parametrize(
    "key",
    [
        Property.CHANGE_DATA_FEED,
        Property.DELETED_FILE_RETENTION_DURATION,
        Property.LOG_RETENTION_DURATION,
        Property.DATA_SKIPPING_NUM_INDEXED_COLS,
        Property.TYPE_WIDENING,
    ],
)
def test_policy_permits_transitions_and_removal_for_unrestricted_properties(key: str) -> None:
    # Then a key with no restricted transitions accepts any change and removal
    assert DELTA_PROPERTY_POLICY.permits_transition(key, observed="anything", desired="else")
    assert DELTA_PROPERTY_POLICY.permits_removal(key, observed="anything")


def test_policy_rejects_a_transition_check_on_an_unmanaged_property() -> None:
    # When a transition check reaches an unmanaged key (a programming error)
    # Then the policy fails rather than answering
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.permits_transition(
            "delta.enableRowTracking",
            observed="true",
            desired="false",
        )


def test_policy_rejects_a_removal_check_on_an_unmanaged_property() -> None:
    # When a removal check reaches an unmanaged key, then the policy fails
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.permits_removal("delta.enableRowTracking", observed="true")
