from types import MappingProxyType

from delta_engine.adapters.databricks.policy import enforce_property_policy
from delta_engine.adapters.schema.delta.properties import Property


def test_enforce_property_policy_filters_and_is_immutable() -> None:
    # Arrange: include two allowed keys, plus two keys that must be dropped
    allowed_key_1 = Property.CHANGE_DATA_FEED.value
    allowed_key_2 = Property.LOG_RETENTION_DURATION.value
    disallowed_key = "not.on.allowlist"
    # Same text but different case — should NOT match (policy is case-sensitive)
    similar_but_wrong_case = allowed_key_1.upper()

    input_properties = {
        allowed_key_1: "true",
        allowed_key_2: "30 days",
        disallowed_key: "x",
        similar_but_wrong_case: "nope",
    }

    # Act
    result = enforce_property_policy(input_properties)

    # Assert
    assert dict(result) == {
        allowed_key_1: "true",
        allowed_key_2: "30 days",
    }
    assert disallowed_key not in result
    assert similar_but_wrong_case not in result


def test_enforce_property_policy_with_empty_input_returns_empty_proxy() -> None:
    # Act
    result = enforce_property_policy({})

    # Assert
    assert isinstance(result, MappingProxyType)
    assert len(result) == 0
