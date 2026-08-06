import pytest

from delta_engine.application.properties import DELTA_PROPERTY_POLICY, Property


def test_property_is_the_expected_public_vocabulary() -> None:
    assert set(Property) == {
        "delta.enableChangeDataFeed",
        "delta.deletedFileRetentionDuration",
        "delta.logRetentionDuration",
        "delta.dataSkippingNumIndexedCols",
        "delta.columnMapping.mode",
        "delta.enableTypeWidening",
    }


def test_policy_rejects_an_unmanaged_declared_property() -> None:
    with pytest.raises(ValueError, match=r"Properties not managed.*delta\.enableRowTracking"):
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
    with pytest.raises(ValueError) as error:
        DELTA_PROPERTY_POLICY.validate_declaration({key: value})

    assert key in str(error.value)
    assert "Expected" in str(error.value)


@pytest.mark.parametrize("key", Property)
def test_policy_accepts_none_as_an_absence_assertion(key: str) -> None:
    DELTA_PROPERTY_POLICY.validate_declaration({key: None})


def test_policy_projects_only_managed_observed_properties() -> None:
    observed = DELTA_PROPERTY_POLICY.project_observed(
        {
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "3",
            "delta.feature.columnMapping": "supported",
        }
    )
    assert dict(observed) == {
        "delta.columnMapping.mode": "name",
    }


def test_policy_permits_only_the_column_mapping_upgrade() -> None:
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
    assert not DELTA_PROPERTY_POLICY.permits_removal(Property.COLUMN_MAPPING_MODE, observed)


def test_policy_permits_first_write_of_a_restricted_property() -> None:
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
    assert DELTA_PROPERTY_POLICY.permits_transition(key, observed="anything", desired="else")
    assert DELTA_PROPERTY_POLICY.permits_removal(key, observed="anything")


def test_policy_rejects_a_transition_check_on_an_unmanaged_property() -> None:
    with pytest.raises(ValueError, match=r"not managed.*delta\.enableRowTracking"):
        DELTA_PROPERTY_POLICY.permits_transition(
            "delta.enableRowTracking",
            observed="true",
            desired="false",
        )


def test_policy_rejects_a_removal_check_on_an_unmanaged_property() -> None:
    with pytest.raises(ValueError, match=r"not managed.*delta\.enableRowTracking"):
        DELTA_PROPERTY_POLICY.permits_removal("delta.enableRowTracking", observed="true")
