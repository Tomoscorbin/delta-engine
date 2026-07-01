from dataclasses import dataclass

from delta_engine.domain.plan.differ import match_by_key


@dataclass(frozen=True)
class _Item:
    name: str
    value: int


def _by_name(item: _Item) -> str:
    return item.name


def test_desired_only_items_are_added():
    # Given an item present only in desired
    desired = [_Item("a", 1)]
    observed: list[_Item] = []

    # When matching by name
    result = match_by_key(desired, observed, key=_by_name)

    # Then it is reported as added and nothing is dropped or common
    assert result.added == (_Item("a", 1),)
    assert result.dropped == ()
    assert result.common == ()


def test_observed_only_items_are_dropped():
    # Given an item present only in observed
    observed = [_Item("a", 1)]

    # When matching against an empty desired
    result = match_by_key([], observed, key=_by_name)

    # Then it is reported as dropped
    assert result.dropped == (_Item("a", 1),)
    assert result.added == ()
    assert result.common == ()


def test_items_present_on_both_sides_are_common_paired_desired_then_observed():
    # Given the same key on each side, carrying different values
    result = match_by_key([_Item("a", 2)], [_Item("a", 1)], key=_by_name)

    # Then it is reported as a (desired, observed) common pair, regardless of value equality
    assert result.common == ((_Item("a", 2), _Item("a", 1)),)
    assert result.added == ()
    assert result.dropped == ()


def test_common_includes_equal_pairs_too():
    # Given the same key with identical values on each side
    result = match_by_key([_Item("a", 1)], [_Item("a", 1)], key=_by_name)

    # Then the pair is still common — the matcher partitions by key, not by equality
    assert result.common == ((_Item("a", 1), _Item("a", 1)),)
    assert result.added == ()
    assert result.dropped == ()


def test_added_preserves_desired_declaration_order():
    # Given several desired-only items in a deliberately non-alphabetical order
    desired = [_Item("b", 1), _Item("a", 1), _Item("c", 1)]

    # When matching against an empty observed
    result = match_by_key(desired, [], key=_by_name)

    # Then added preserves desired order, not sorted order
    assert [item.name for item in result.added] == ["b", "a", "c"]


def test_dropped_preserves_observed_declaration_order():
    # Given several observed-only items in a deliberately non-alphabetical order
    observed = [_Item("b", 1), _Item("a", 1), _Item("c", 1)]

    # When matching against an empty desired
    result = match_by_key([], observed, key=_by_name)

    # Then dropped preserves observed order, not sorted order
    assert [item.name for item in result.dropped] == ["b", "a", "c"]
