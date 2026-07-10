from hypothesis import given, strategies as st
import pytest

from delta_engine.domain.model import Column, DesiredTable, Integer, Long, QualifiedName
from delta_engine.domain.plan.actions import (
    Action,
    ActionPhase,
    ActionPlan,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
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
        (_create_table_action(), ""),
        (AddColumn(column=_column("x")), "x"),
        (DropColumn(column_name="x"), "x"),
        (SetProperty(name="prop", value="1"), "prop"),
        (UnsetProperty(name="prop"), "prop"),
        (SetTableTag(name="env", value="prod"), "env"),
        (UnsetTableTag(name="env"), "env"),
        (SetColumnTag(column_name="email", name="pii", value="true"), "email.pii"),
        (UnsetColumnTag(column_name="email", name="pii"), "email.pii"),
        (SetColumnComment(column_name="email", comment="customer email"), "email"),
        (SetTableComment(comment="table comment"), ""),
        (SetColumnNullability(column_name="email", nullable=False), "email"),
        (DropPrimaryKey(), ""),
        (SetPrimaryKey(columns=("id",), constraint_name="table_pk"), ""),
        (DropForeignKey(constraint_name="orders_customer_id_fk"), "orders_customer_id_fk"),
        (
            SetForeignKey(
                local_columns=("customer_id",),
                referenced_table=QualifiedName("cat", "sch", "customers"),
                referenced_columns=("id",),
                constraint_name="orders_customer_id_fk",
            ),
            "customer_id",
        ),
        (AlterClustering(columns=("region",)), ""),
        (AlterColumnType(column_name="id", data_type=Long(), observed_type=Integer()), "id"),
    ],
)
def test_action_subject_identifies_the_within_phase_target(action: Action, expected_subject: str):
    # Then each action exposes the target used for deterministic ordering within its phase
    assert action.subject == expected_subject


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


def test_plan_orders_set_primary_key_after_set_column_nullability():
    # Given a SetPrimaryKey and a SetColumnNullability in the same plan
    plan = ActionPlan(
        (
            SetPrimaryKey(columns=("id",), constraint_name="tbl_pk"),
            SetColumnNullability(column_name="id", nullable=False),
        )
    )

    # Then SetColumnNullability runs first
    assert [type(a) for a in plan] == [SetColumnNullability, SetPrimaryKey]


def test_plan_full_phase_order_with_all_action_types():
    # Given one action from each phase, handed to the plan in scrambled order
    plan = ActionPlan(
        (
            SetPrimaryKey(columns=("id",), constraint_name="tbl_pk"),
            SetForeignKey(
                local_columns=("customer_id",),
                referenced_table=QualifiedName("cat", "sch", "customers"),
                referenced_columns=("id",),
                constraint_name="tbl_customer_id_fk",
            ),
            SetTableComment(comment="tbl comment"),
            AddColumn(column=_column("a_col")),
            SetProperty(name="p_set", value="1"),
            UnsetProperty(name="p_unset"),
            SetColumnNullability(column_name="nn_col", nullable=False),
            DropForeignKey(constraint_name="t_old_fk"),
            DropPrimaryKey(),
            DropColumn(column_name="d_col"),
            SetColumnTag(column_name="email", name="pii", value="true"),
            UnsetColumnTag(column_name="email", name="old"),
            SetColumnComment(column_name="c_col", comment="c"),
            _create_table_action(),
            SetTableTag(name="env", value="prod"),
            UnsetTableTag(name="old_tag"),
            AlterClustering(columns=("region",)),
            AlterColumnType(column_name="w_col", data_type=Long(), observed_type=Integer()),
        )
    )

    # Then the plan holds them in the documented phase precedence
    assert [type(a) for a in plan] == [
        CreateTable,
        SetProperty,
        UnsetProperty,
        SetTableTag,
        UnsetTableTag,
        DropForeignKey,
        DropPrimaryKey,
        AddColumn,
        AlterColumnType,
        AlterClustering,
        DropColumn,
        SetColumnTag,
        UnsetColumnTag,
        SetColumnComment,
        SetTableComment,
        SetColumnNullability,
        SetPrimaryKey,
        SetForeignKey,
    ]


# ----- DropForeignKey / SetForeignKey


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


# ----- SetColumnTag / UnsetColumnTag


def test_plan_orders_set_column_tag_after_add_column():
    # Given an AddColumn and a SetColumnTag on that column in one plan
    plan = ActionPlan(
        (
            SetColumnTag(column_name="email", name="pii", value="true"),
            AddColumn(column=_column("email")),
        )
    )

    # Then the column is added before it is tagged
    assert [type(a) for a in plan] == [AddColumn, SetColumnTag]


def test_set_property_observed_value_defaults_to_none():
    # Given a SetProperty built without an observed value (first write)
    action = SetProperty(name="delta.enableChangeDataFeed", value="true")

    assert action.observed_value is None


def test_drop_foreign_key_phases_before_drop_primary_key():
    # PrimaryKeyReferencedByForeignKeys exempts a same-table FK dropped in the
    # same sync ONLY because the plan drops foreign keys before primary keys.
    # Reordering these phases would make that exemption approve plans that
    # fail at execution.
    assert ActionPhase.DROP_FOREIGN_KEY < ActionPhase.DROP_PRIMARY_KEY


def test_plan_orders_alter_column_type_after_property_set_and_before_key_set():
    # Given a widen, its enabling property, and a primary-key set, supplied shuffled
    plan = ActionPlan(
        (
            SetPrimaryKey(columns=("id",), constraint_name="tbl_id_pk"),
            AlterColumnType(column_name="id", data_type=Long(), observed_type=Integer()),
            SetProperty(name="delta.enableTypeWidening", value="true"),
        )
    )

    # Then the property lands before the widen, and the widen before the key
    assert [type(action) for action in plan] == [SetProperty, AlterColumnType, SetPrimaryKey]


def test_plan_orders_alter_clustering_after_add_column():
    # Given a new column that is also the clustering key
    plan = ActionPlan(
        (
            AlterClustering(columns=("region",)),
            AddColumn(column=_column("region")),
        )
    )
    # Then the column is added before it is used as a clustering key
    assert [type(a) for a in plan] == [AddColumn, AlterClustering]


def test_plan_reclusters_before_dropping_a_column():
    # Given a sync that drops the current clustering-key column and reclusters
    # onto another column
    plan = ActionPlan(
        (
            DropColumn(column_name="region"),
            AlterClustering(columns=("id",)),
        )
    )
    # Then clustering is changed off the old key before that column is dropped,
    # so the drop never targets a live clustering key
    assert [type(a) for a in plan] == [AlterClustering, DropColumn]
