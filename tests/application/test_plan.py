from delta_engine.application.plan import make_plan_context
from delta_engine.domain.model import Column, DesiredTable, Integer, ObservedTable, QualifiedName
from delta_engine.domain.plan.actions import (
    ActionPhase,
    CreateTable,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def test_context_carries_desired_and_observed():
    # Given a desired and an observed definition
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))
    observed = ObservedTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))

    # When building the plan context
    context = make_plan_context(desired, observed)

    # Then it exposes both states it was built from
    assert context.desired is desired
    assert context.observed is observed


def test_missing_observed_plans_a_create_table():
    # Given a desired table and no observed table
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))

    # When building the plan context
    context = make_plan_context(desired, observed=None)

    # Then the plan creates the table
    assert context.plan.actions == (CreateTable(desired),)


def test_plan_actions_are_ordered_by_execution_phase():
    # Given an existing table whose diff produces actions across several phases:
    # a new column to add, a legacy column to drop, and a table comment to set
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()), Column("age", Integer())),
        comment="core table",
    )
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()), Column("legacy", Integer())),
        comment="",
    )

    # When building the plan context
    context = make_plan_context(desired, observed)

    # Then actions come out in non-decreasing execution-phase order
    phases = [action.phase for action in context.plan.actions]
    assert phases == sorted(phases)
    # And the phases present are exactly those the diff produced
    assert set(phases) == {
        ActionPhase.ADD_COLUMN,
        ActionPhase.DROP_COLUMN,
        ActionPhase.SET_TABLE_COMMENT,
    }


def test_empty_diff_produces_an_empty_plan():
    # Given identical desired and observed definitions
    columns = (Column("id", Integer()),)
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=columns)
    observed = ObservedTable(qualified_name=_QUALIFIED_NAME, columns=columns)

    # When building the plan context
    context = make_plan_context(desired, observed)

    # Then there is nothing to do
    assert context.plan.actions == ()
