"""Assertions for observable action payloads, independent of action identity."""

from collections.abc import Iterable, Mapping, Sequence, Set
from dataclasses import fields, is_dataclass

from delta_engine.domain.plan import Action


def _observable_state(value: object) -> object:
    """Project nested immutable values to their exact public dataclass state."""
    if is_dataclass(value) and not isinstance(value, type):
        return (
            type(value),
            tuple(
                (field.name, _observable_state(getattr(value, field.name)))
                for field in fields(value)
                if not field.name.startswith("_")
            ),
        )
    if isinstance(value, Mapping):
        items = ((_observable_state(key), _observable_state(item)) for key, item in value.items())
        return (type(value), tuple(sorted(items, key=repr)))
    if isinstance(value, Set):
        return (type(value), tuple(sorted((_observable_state(item) for item in value), key=repr)))
    if isinstance(value, Sequence) and not isinstance(value, str):
        return (type(value), tuple(_observable_state(item) for item in value))
    if isinstance(value, str):
        return (type(value), str(value))
    return value


def assert_action_sequence(actual: Sequence[Action], expected: Sequence[Action]) -> None:
    """Assert action types and payloads without giving actions value equality."""
    assert _observable_state(tuple(actual)) == _observable_state(tuple(expected))


def assert_action_set(actual: Iterable[Action], expected: Iterable[Action]) -> None:
    """Assert an unordered collection's exact action types and payloads."""
    actual_states = sorted((_observable_state(action) for action in actual), key=repr)
    expected_states = sorted((_observable_state(action) for action in expected), key=repr)
    assert actual_states == expected_states
