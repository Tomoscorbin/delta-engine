import pytest

from delta_engine.domain.model.property import PropertyDefinition


def test_definition_defaults_describe_an_unrestricted_key():
    # Given a definition with only a key
    definition = PropertyDefinition(key="example.key")

    # Then it is unrestricted and unsettable
    assert definition.permitted_transitions == frozenset()
    assert definition.unset_permitted is True


def test_definition_is_immutable():
    definition = PropertyDefinition(key="example.key")

    with pytest.raises(AttributeError):
        definition.key = "other"  # type: ignore[misc]
