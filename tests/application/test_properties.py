import pytest

from delta_engine.application.properties import DELTA_PROPERTY_POLICY
from delta_engine.domain.model.property import TableProperty


def test_policy_rejects_an_unmanaged_declared_property() -> None:
    # When a declaration carries a key the engine does not manage
    # Then validation fails
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.validate_declaration({"delta.enableRowTracking": "true"})


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (TableProperty.CHANGE_DATA_FEED, "true"),
        (TableProperty.CHANGE_DATA_FEED, "false"),
        (TableProperty.COLUMN_MAPPING_MODE, "name"),
        (TableProperty.COLUMN_MAPPING_MODE, "none"),
        (TableProperty.LOG_RETENTION_DURATION, "interval 30 days"),
        (TableProperty.DELETED_FILE_RETENTION_DURATION, "interval 7 days"),
        (TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS, "-1"),
        (TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS, "32"),
        (TableProperty.TYPE_WIDENING, "true"),
        (TableProperty.TYPE_WIDENING, "false"),
    ],
)
def test_policy_accepts_valid_property_values(key: str, value: str) -> None:
    # Then a canonical value for each managed key is accepted
    DELTA_PROPERTY_POLICY.validate_declaration({key: value})


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (TableProperty.CHANGE_DATA_FEED, "True"),
        (TableProperty.CHANGE_DATA_FEED, "yes"),
        (TableProperty.COLUMN_MAPPING_MODE, "id"),
        (TableProperty.LOG_RETENTION_DURATION, "30 days"),
        (TableProperty.LOG_RETENTION_DURATION, "INTERVAL 1 WEEK"),
        (TableProperty.LOG_RETENTION_DURATION, "interval thirty days"),
        (TableProperty.LOG_RETENTION_DURATION, "interval 1 hour 30 minutes"),
        (TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS, "-2"),
        (TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS, "many"),
        (TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS, "1_000"),
        (TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS, "+5"),
        (TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS, " 5 "),
        (TableProperty.TYPE_WIDENING, "True"),
        (TableProperty.TYPE_WIDENING, "enabled"),
    ],
)
def test_policy_rejects_invalid_property_values(key: str, value: str) -> None:
    # When a declared value is not the catalog's canonical spelling
    # Then validation fails — a non-canonical value would re-diff as drift on
    # every sync, or fail Java-side parsing at execution instead of declaration
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.validate_declaration({key: value})


@pytest.mark.parametrize("key", TableProperty)
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
    assert not DELTA_PROPERTY_POLICY.permits_transition(
        TableProperty.COLUMN_MAPPING_MODE,
        observed=observed,
        desired=None,
    )


def test_policy_permits_first_write_of_a_restricted_property() -> None:
    # Then a key absent from the catalog may always be written
    assert DELTA_PROPERTY_POLICY.permits_transition(
        TableProperty.COLUMN_MAPPING_MODE,
        observed=None,
        desired="name",
    )


@pytest.mark.parametrize(
    "key",
    [
        TableProperty.CHANGE_DATA_FEED,
        TableProperty.DELETED_FILE_RETENTION_DURATION,
        TableProperty.LOG_RETENTION_DURATION,
        TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS,
        TableProperty.TYPE_WIDENING,
    ],
)
def test_policy_permits_transitions_and_removal_for_unrestricted_properties(key: str) -> None:
    # Then a key with no restricted transitions accepts any change and removal
    assert DELTA_PROPERTY_POLICY.permits_transition(key, observed="anything", desired="else")
    assert DELTA_PROPERTY_POLICY.permits_transition(key, observed="anything", desired=None)


def test_policy_rejects_a_transition_check_on_an_unmanaged_property() -> None:
    # When a transition check reaches an unmanaged key (a programming error)
    # Then the policy fails rather than answering
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.permits_transition(
            "delta.enableRowTracking",
            observed="true",
            desired="false",
        )
