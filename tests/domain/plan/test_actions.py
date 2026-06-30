from hypothesis import given, strategies as st
import pytest

from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.plan.actions import (
    Action,
    ActionPhase,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetForeignKey,
    SetPrimaryKey,
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


# ----- ActionPlan: permutation invariance


_SAMPLE_ACTIONS: list[Action] = [
    AddColumn(column=_column("alpha")),
    AddColumn(column=_column("beta")),
    DropColumn(column_name="gamma"),
    SetProperty(name="k1", value="v1"),
    SetProperty(name="k2", value="v2"),
    SetColumnComment(column_name="delta", comment="c"),
    SetTableComment(comment="tbl"),
    SetColumnNullability(column_name="epsilon", nullable=False),
]


@given(st.permutations(_SAMPLE_ACTIONS))
def test_actionplan_order_is_independent_of_input_permutation(
    shuffled: list[Action],
) -> None:
    # Given: the canonical plan built from a fixed action list
    canonical = ActionPlan(tuple(_SAMPLE_ACTIONS))

    # When: the same actions are supplied in an arbitrary permutation
    result = ActionPlan(tuple(shuffled))

    # Then: both plans hold the same actions in the same execution order
    assert tuple(result) == tuple(canonical)


# ----- DropPrimaryKey / SetPrimaryKey


def test_drop_primary_key_has_no_subject():
    # Given a DropPrimaryKey action (whole-table operation)
    action = DropPrimaryKey()

    # Then it has no within-phase subject
    assert action.subject == ""


def test_set_primary_key_has_no_subject():
    # Given a SetPrimaryKey action
    action = SetPrimaryKey(
        columns=(Column(name="id", data_type=Integer(), nullable=False),),
        constraint_name="orders_pk",
    )

    # Then it has no within-phase subject
    assert action.subject == ""


def test_plan_orders_drop_primary_key_before_add_column():
    # Given a DropPrimaryKey and an AddColumn in the same plan
    plan = ActionPlan(
        (
            AddColumn(column=_column("new_col")),
            DropPrimaryKey(),
        )
    )

    # Then DropPrimaryKey runs first
    assert [type(a) for a in plan] == [DropPrimaryKey, AddColumn]


def test_plan_orders_set_primary_key_after_set_column_nullability():
    # Given a SetPrimaryKey and a SetColumnNullability in the same plan
    plan = ActionPlan(
        (
            SetPrimaryKey(
                columns=(Column(name="id", data_type=Integer(), nullable=False),),
                constraint_name="t_pk",
            ),
            SetColumnNullability(column_name="id", nullable=False),
        )
    )

    # Then SetColumnNullability runs first
    assert [type(a) for a in plan] == [SetColumnNullability, SetPrimaryKey]


def test_plan_full_phase_order_with_all_action_types():
    # Given one action from each phase, handed to the plan in scrambled order
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )
    plan = ActionPlan(
        (
            SetPrimaryKey(
                columns=(Column(name="id", data_type=Integer(), nullable=False),),
                constraint_name="t_pk",
            ),
            SetForeignKey(foreign_key=fk, constraint_name="t_customer_id_fk"),
            SetTableComment(comment="tbl comment"),
            AddColumn(column=_column("a_col")),
            SetProperty(name="p_set", value="1"),
            SetColumnNullability(column_name="nn_col", nullable=False),
            DropForeignKey(constraint_name="t_old_fk"),
            DropPrimaryKey(),
            DropColumn(column_name="d_col"),
            SetColumnComment(column_name="c_col", comment="c"),
            _create_table_action(),
        )
    )

    # Then the plan holds them in the documented phase precedence
    assert [type(a) for a in plan] == [
        CreateTable,
        SetProperty,
        DropForeignKey,
        DropPrimaryKey,
        AddColumn,
        DropColumn,
        SetColumnComment,
        SetTableComment,
        SetColumnNullability,
        SetPrimaryKey,
        SetForeignKey,
    ]


# ----- DropForeignKey / SetForeignKey


def test_drop_foreign_key_phase_is_before_drop_primary_key():
    # A self-referential FK references the table's own primary key, so the FK
    # must be dropped before the primary key it depends on can be dropped.
    assert ActionPhase.DROP_FOREIGN_KEY < ActionPhase.DROP_PRIMARY_KEY


def test_drop_foreign_key_phase_is_before_drop_column():
    # Databricks rejects dropping a column still referenced by an active FK, so
    # the FK must be dropped first.
    assert ActionPhase.DROP_FOREIGN_KEY < ActionPhase.DROP_COLUMN


def test_set_foreign_key_phase_is_after_set_primary_key():
    # A FK references a primary/unique key, so the referenced key must be set
    # before the FK that points at it.
    assert ActionPhase.SET_FOREIGN_KEY > ActionPhase.SET_PRIMARY_KEY


def test_set_foreign_key_phase_is_after_drop_foreign_key():
    assert ActionPhase.SET_FOREIGN_KEY > ActionPhase.DROP_FOREIGN_KEY


def test_set_foreign_key_phase_is_before_column_type_change():
    assert ActionPhase.SET_FOREIGN_KEY < ActionPhase.COLUMN_TYPE_CHANGE


def test_plan_orders_drop_foreign_key_before_drop_column():
    # Given a DropColumn and the DropForeignKey for a constraint on that column
    plan = ActionPlan(
        (
            DropColumn(column_name="customer_id"),
            DropForeignKey(constraint_name="orders_customer_id_fk"),
        )
    )

    # Then the foreign key is dropped before the column it references
    assert [type(a) for a in plan] == [DropForeignKey, DropColumn]


def test_plan_orders_drop_foreign_key_before_drop_primary_key():
    # Given a DropPrimaryKey and a DropForeignKey in the same plan
    plan = ActionPlan(
        (
            DropPrimaryKey(),
            DropForeignKey(constraint_name="employees_manager_id_fk"),
        )
    )

    # Then the foreign key is dropped before the primary key it may reference
    assert [type(a) for a in plan] == [DropForeignKey, DropPrimaryKey]


def test_drop_foreign_key_subject_is_constraint_name():
    # Given
    action = DropForeignKey(constraint_name="orders_customer_id_fk")

    # Then subject is the constraint name (for deterministic ordering within the phase)
    assert action.subject == "orders_customer_id_fk"


def test_set_foreign_key_subject_is_constraint_name():
    # Given
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )
    action = SetForeignKey(foreign_key=fk, constraint_name="orders_customer_id_fk")

    # Then subject is the constraint name
    assert action.subject == "orders_customer_id_fk"
