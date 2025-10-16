import pytest

from delta_engine.application.ordering import action_sort_key, subject_name
from delta_engine.domain.model import Column, DesiredTable, QualifiedName, TableFormat
from delta_engine.domain.plan.actions import (
    AddColumn,
    CreateTable,
    DropColumn,
    PartitionBy,
    SetColumnComment,
    SetColumnNullability,
    SetProperty,
    SetTableComment,
    UnsetProperty,
)

# ----- builders


def _column(name: str) -> Column:
    return Column(name=name, data_type="string", nullable=True, comment=None)


def _create_table_action() -> CreateTable:
    dt = DesiredTable(
        qualified_name=QualifiedName("c", "s", "t"),
        columns=(_column("id"),),
        comment=None,
        properties={},
        partitioned_by=(),
        format=TableFormat.DELTA,
    )
    return CreateTable(table=dt)


# ------ tests


def test_orders_by_phase_in_documented_precedence():
    # Given one action from each phase, in scrambled order
    actions = (
        UnsetProperty(name="p_unset"),
        SetTableComment(comment="tbl comment"),
        AddColumn(column=_column("a_col")),
        PartitionBy(column_names=("ds",)),
        SetProperty(name="p_set", value="1"),
        SetColumnNullability(column_name="nn_col", nullable=False),
        DropColumn(column_name="d_col"),
        SetColumnComment(column_name="c_col", comment="c"),
        _create_table_action(),
    )

    # When sorting using the production key
    ordered = tuple(sorted(actions, key=action_sort_key))

    # Then the overall order follows the documented phase precedence strictly
    expected_types_in_order = [
        CreateTable,
        SetProperty,
        AddColumn,
        DropColumn,
        SetColumnComment,
        SetTableComment,
        UnsetProperty,
        SetColumnNullability,
        PartitionBy,
    ]
    assert [type(a) for a in ordered] == expected_types_in_order


def test_within_phase_actions_are_ordered_by_subject_name():
    # Given two AddColumn actions in the same phase with different names
    a1 = AddColumn(column=_column("b_col"))
    a2 = AddColumn(column=_column("a_col"))

    # When sorting
    ordered = tuple(sorted((a1, a2), key=action_sort_key))

    # Then the earlier subject name ("a_col") comes first within the phase
    assert [subject_name(a) for a in ordered] == ["a_col", "b_col"]


@pytest.mark.parametrize(
    "action, expected_subject",
    [
        (AddColumn(column=_column("xcol")), "xcol"),
        (DropColumn(column_name="ycol"), "ycol"),
        (SetProperty(name="propA", value="v"), "propA"),
        (UnsetProperty(name="propB"), "propB"),
        (SetColumnComment(column_name="zcol", comment="c"), "zcol"),
        (SetColumnNullability(column_name="ncol", nullable=False), "ncol"),
        (SetTableComment(comment="table comment"), ""),  # no subject -> empty string
        (PartitionBy(column_names=("ds",)), ""),  # current behaviour: no subject name
    ],
)
def test_subject_name_extraction_matches_action_type_contract(action, expected_subject):
    # Given an action
    # When extracting the subject name
    # Then we get the identifier used for within-phase ordering
    assert subject_name(action) == expected_subject


def test_sort_is_stable_when_phase_and_subject_tie():
    # Given two SetProperty actions with the same name (identical phase+subject key)
    a1 = SetProperty(name="alpha", value="1")
    a2 = SetProperty(name="alpha", value="2")  # same subject; different non-subject field

    # When sorting
    ordered = tuple(sorted((a1, a2), key=action_sort_key))

    # Then the original relative order is preserved (sort is stable)
    assert ordered == (a1, a2)


def test_non_subject_fields_do_not_influence_order_within_phase():
    # Given SetProperty actions where subjects differ but values (non-subject) might mislead
    s1 = SetProperty(name="a_key", value="zzz")
    s2 = SetProperty(name="b_key", value="aaa")

    # And AddColumn actions where data_type/nullability differ but names decide order
    c1 = AddColumn(column=Column(name="a_col", data_type="int", nullable=False, comment=None))
    c2 = AddColumn(column=Column(name="b_col", data_type="string", nullable=True, comment=None))

    # When sorting each pair
    ordered_props = tuple(sorted((s1, s2), key=action_sort_key))
    ordered_cols = tuple(sorted((c2, c1), key=action_sort_key))  # deliberately reversed input

    # Then only the subject (name) controls order; non-subject fields have no effect
    assert [subject_name(a) for a in ordered_props] == ["a_key", "b_key"]
    assert [subject_name(a) for a in ordered_cols] == ["a_col", "b_col"]
