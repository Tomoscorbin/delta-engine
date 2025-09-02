from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.model.table import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import SetProperty, SetTableComment, UnsetProperty
from delta_engine.domain.services.table_diff import (
    _diff_properties_for_sets,
    _diff_properties_for_unsets,
    _properties_to_actions,
    diff_properties,
    diff_table_comments,
)
from tests.factories import make_qualified_name

# --- _properties_to_actions ---------------------------------------------------


def test_properties_to_actions_converts_all_pairs() -> None:
    props = {"owner": "data-eng", "quality": "gold", "pii": "no"}

    actions = _properties_to_actions(props)

    assert len(actions) == 3
    actual = {(a.name, a.value) for a in actions}
    assert actual == {("owner", "data-eng"), ("quality", "gold"), ("pii", "no")}


def test_properties_to_actions_empty_mapping_returns_empty_tuple() -> None:
    assert _properties_to_actions({}) == ()


# --- _diff_properties_for_sets ------------------------------------------------


def test_diff_properties_for_sets_detects_missing_and_changed_values() -> None:
    desired = {"a": "1", "b": "2", "c": "3"}
    observed = {"a": "0", "b": "2"}

    actions = _diff_properties_for_sets(desired, observed)

    actual = {(a.name, a.value) for a in actions}
    assert actual == {("a", "1"), ("c", "3")}


def test_diff_properties_for_sets_no_changes_returns_empty_tuple() -> None:
    desired = {"a": "1", "b": "2"}
    observed = {"a": "1", "b": "2"}

    assert _diff_properties_for_sets(desired, observed) == ()


# --- _diff_properties_for_unsets ---------------------------------------------


def test_diff_properties_for_unsets_returns_unsets_for_observed_extras() -> None:
    desired = {"keep": "yes"}
    observed = {"keep": "yes", "ttl": "7d", "owner": "ops"}

    actions = _diff_properties_for_unsets(desired, observed)

    assert {a.name for a in actions} == {"ttl", "owner"}


def test_diff_properties_for_unsets_no_extras_returns_empty_tuple() -> None:
    desired = {"a": "1", "b": "2"}
    observed = {"a": "1"}

    assert _diff_properties_for_unsets(desired, observed) == ()


# --- diff_properties (composition, order-agnostic) ----------------------------


def test_diff_properties_combines_sets_and_unsets() -> None:
    desired = {"a": "1", "b": "2"}  # set a (changed), set b (missing)
    observed = {"a": "0", "c": "3"}  # unset c (extra)

    actions = diff_properties(desired, observed)

    # All actions are SetProperty or UnsetProperty.
    assert all(isinstance(a, (SetProperty, UnsetProperty)) for a in actions)

    set_pairs = {(a.name, a.value) for a in actions if isinstance(a, SetProperty)}
    unset_names = {a.name for a in actions if isinstance(a, UnsetProperty)}

    assert set_pairs == {("a", "1"), ("b", "2")}
    assert unset_names == {"c"}


def test_diff_properties_no_differences_returns_empty_tuple() -> None:
    desired = {"x": "y"}
    observed = {"x": "y"}

    assert diff_properties(desired, observed) == ()


def test_diff_properties_all_unsets_when_desired_is_empty() -> None:
    desired = {}
    observed = {"ttl": "7d", "owner": "ops"}

    actions = diff_properties(desired, observed)

    assert {(a.name, getattr(a, "value", None)) for a in actions} == {
        ("ttl", None),
        ("owner", None),
    }
    assert all(isinstance(a, UnsetProperty) for a in actions)


def test_diff_properties_all_sets_when_observed_is_empty() -> None:
    desired = {"a": "1", "b": "2"}
    observed = {}

    actions = diff_properties(desired, observed)

    assert {(a.name, a.value) for a in actions if isinstance(a, SetProperty)} == {
        ("a", "1"),
        ("b", "2"),
    }
    assert not [a for a in actions if isinstance(a, UnsetProperty)]


# --- diff_table_comments ------------------------------------------------------


def test_diff_table_comments_noop_when_comments_match() -> None:
    name = make_qualified_name("dev", "silver", "people")
    cols = (Column("id", Integer()),)
    desired = DesiredTable(name, cols, comment="team-owned")
    observed = ObservedTable(name, cols, comment="team-owned")

    assert diff_table_comments(desired, observed) == ()


def test_diff_table_comments_sets_desired_comment_when_different() -> None:
    name = make_qualified_name("dev", "silver", "people")
    cols = (Column("id", Integer()),)
    desired = DesiredTable(name, cols, comment="gold quality")
    observed = ObservedTable(name, cols, comment="old comment")

    actions = diff_table_comments(desired, observed)

    assert actions == (SetTableComment(comment="gold quality"),)
