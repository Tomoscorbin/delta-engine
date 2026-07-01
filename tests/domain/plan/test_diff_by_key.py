from dataclasses import dataclass

from delta_engine.domain.plan.differ import diff_by_key


@dataclass(frozen=True)
class _Item:
    name: str
    value: int


def _by_name(item: _Item) -> str:
    return item.name


def _value_equal(desired: _Item, observed: _Item) -> bool:
    return desired.value == observed.value


def test_desired_only_items_are_added():
    # Given an item present only in desired
    desired = [_Item("a", 1)]
    observed: list[_Item] = []

    # When diffing by name
    result = diff_by_key(desired, observed, key=_by_name, equals=_value_equal)

    # Then it is reported as added and nothing is dropped or changed
    assert result.added == (_Item("a", 1),)
    assert result.dropped == ()
    assert result.changed == ()


def test_observed_only_items_are_dropped():
    # Given an item present only in observed
    result = diff_by_key([], [_Item("a", 1)], key=_by_name, equals=_value_equal)

    # Then it is reported as dropped
    assert result.dropped == (_Item("a", 1),)
    assert result.added == ()
    assert result.changed == ()


def test_items_in_both_that_differ_are_changed():
    # Given the same key with a different value on each side
    result = diff_by_key([_Item("a", 2)], [_Item("a", 1)], key=_by_name, equals=_value_equal)

    # Then it is reported as a (desired, observed) changed pair
    assert result.changed == ((_Item("a", 2), _Item("a", 1)),)
    assert result.added == ()
    assert result.dropped == ()


def test_items_in_both_that_are_equal_produce_no_outcome():
    # Given the same key with equal values on each side
    result = diff_by_key([_Item("a", 1)], [_Item("a", 1)], key=_by_name, equals=_value_equal)

    # Then there is no add, drop, or change (noop is the implicit complement)
    assert result.added == ()
    assert result.dropped == ()
    assert result.changed == ()


def test_added_preserves_desired_declaration_order():
    # Given several desired-only items
    desired = [_Item("b", 1), _Item("a", 1), _Item("c", 1)]

    # When diffing against an empty observed
    result = diff_by_key(desired, [], key=_by_name, equals=_value_equal)

    # Then added preserves desired order, not sorted order
    assert [item.name for item in result.added] == ["b", "a", "c"]
