import pytest

from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetColumnNullability,
    SetProperty,
    SetTableComment,
)

# ----- builders


def _column(name: str) -> Column:
    return Column(name=name, data_type=Integer())


def _create_table_action() -> CreateTable:
    table = DesiredTable(
        qualified_name=QualifiedName("c", "s", "t"),
        columns=(_column("id"),),
    )
    return CreateTable(table=table)


# ----- ActionPlan: truthiness / length


def test_actionplan_truthiness_and_length():
    # Given empty and non-empty plans
    empty = ActionPlan(())
    non_empty = ActionPlan((DropColumn("legacy"),))

    # Then bool/len reflect the action count
    assert bool(empty) is False
    assert len(empty) == 0
    assert bool(non_empty) is True
    assert len(non_empty) == 1


# ----- ActionPlan: orders its own actions on construction


def test_plan_orders_actions_by_phase_in_documented_precedence():
    # Given one action from each phase, handed to the plan in scrambled order
    plan = ActionPlan(
        (
            SetTableComment(comment="tbl comment"),
            AddColumn(column=_column("a_col")),
            SetProperty(name="p_set", value="1"),
            SetColumnNullability(column_name="nn_col", nullable=False),
            DropColumn(column_name="d_col"),
            SetColumnComment(column_name="c_col", comment="c"),
            _create_table_action(),
        )
    )

    # Then the plan holds them in the documented phase precedence
    assert [type(a) for a in plan] == [
        CreateTable,
        SetProperty,
        AddColumn,
        DropColumn,
        SetColumnComment,
        SetTableComment,
        SetColumnNullability,
    ]


def test_plan_orders_within_a_phase_by_subject_name():
    # Given two same-phase actions handed in reverse subject order
    plan = ActionPlan((AddColumn(column=_column("b_col")), AddColumn(column=_column("a_col"))))

    # Then the earlier subject name comes first
    assert [a.subject for a in plan] == ["a_col", "b_col"]


def test_plan_ordering_is_stable_when_phase_and_subject_tie():
    # Given two actions with an identical (phase, subject) key
    first = SetProperty(name="alpha", value="1")
    second = SetProperty(name="alpha", value="2")

    # Then their original relative order is preserved
    plan = ActionPlan((first, second))
    assert tuple(plan) == (first, second)


def test_plan_ordering_ignores_non_subject_fields():
    # Given actions whose non-subject fields (value, data type) might mislead a sort
    plan = ActionPlan(
        (
            SetProperty(name="b_key", value="aaa"),
            SetProperty(name="a_key", value="zzz"),
        )
    )

    # Then only the subject controls order
    assert [a.subject for a in plan] == ["a_key", "b_key"]


# ----- Action.subject contract


@pytest.mark.parametrize(
    "action, expected_subject",
    [
        (AddColumn(column=_column("xcol")), "xcol"),
        (DropColumn(column_name="ycol"), "ycol"),
        (SetProperty(name="propA", value="v"), "propA"),
        (SetColumnComment(column_name="zcol", comment="c"), "zcol"),
        (SetColumnNullability(column_name="ncol", nullable=False), "ncol"),
        (SetTableComment(comment="table comment"), ""),  # whole-table action: no subject
    ],
)
def test_subject_identifies_within_phase_target(action, expected_subject):
    # Then a column/property action's subject is its name; a whole-table action's is empty
    assert action.subject == expected_subject


def test_create_table_action_has_no_subject():
    # Given a CreateTable action (targets the table as a whole)
    # Then it has no within-phase subject
    assert _create_table_action().subject == ""
