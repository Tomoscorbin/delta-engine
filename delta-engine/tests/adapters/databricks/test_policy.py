from types import MappingProxyType

import pytest

from delta_engine.adapters.databricks.policy import PropertyPolicy

TEST_ALLOWLIST = frozenset({"delta.allowed.a", "delta.allowed.b"})
TEST_POLICY = PropertyPolicy(TEST_ALLOWLIST)


def test_property_policy_filters_and_is_readonly():
    # Given a properties dict with two allowed keys, a disallowed key, and a wrong-case variant
    props_in = {
        "delta.allowed.a": "yes",
        "delta.allowed.b": "42",
        "delta.not.allowed": "no",
        "DELTA.ALLOWED.A": "nope",  # case-sensitive, should be dropped
    }

    # When we enforce the test-local policy
    result = TEST_POLICY.enforce(props_in)

    # Then only allowed keys remain and mapping is read-only
    assert dict(result) == {"delta.allowed.a": "yes", "delta.allowed.b": "42"}
    assert isinstance(result, MappingProxyType)
    with pytest.raises(TypeError):
        result["x"] = "y"


def test_property_policy_on_empty_input_returns_empty_proxy():
    # Given an empty properties dict
    props_in: dict[str, str] = {}

    # When we enforce the policy
    result = TEST_POLICY.enforce(props_in)

    # Then we get an empty, read-only mapping
    assert isinstance(result, MappingProxyType)
    assert dict(result) == {}
    with pytest.raises(TypeError):
        result["x"] = "y"
