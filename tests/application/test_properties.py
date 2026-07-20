import pytest

from delta_engine.application.properties import DELTA_PROPERTY_POLICY, Property


def test_policy_rejects_an_unmanaged_declared_property():
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.validate_declaration({"delta.enableRowTracking": "true"})


def test_policy_rejects_an_invalid_managed_value():
    with pytest.raises(ValueError):
        DELTA_PROPERTY_POLICY.validate_declaration({Property.CHANGE_DATA_FEED: "yes"})


def test_policy_accepts_none_as_an_absence_assertion():
    DELTA_PROPERTY_POLICY.validate_declaration({Property.CHANGE_DATA_FEED: None})


def test_policy_projects_only_unmanaged_observed_properties():
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


def test_policy_permits_only_the_column_mapping_upgrade():
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


def test_policy_does_not_permit_column_mapping_removal():
    assert not DELTA_PROPERTY_POLICY.permits_removal(
        "delta.columnMapping.mode",
        observed="name",
    )
