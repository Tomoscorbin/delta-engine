from hypothesis import given, strategies as st
import pytest

from delta_engine.domain.model import (
    Column,
    DesiredTable,
    ForeignKeyConstraint,
    Integer,
    Long,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
    TableAspect,
)
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
    RenameColumn,
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
from delta_engine.domain.plan.diff import ColumnRenameConflict

_TARGET = QualifiedName("cat", "sch", "table")


def _column(name: str) -> Column:
    return Column(name=name, data_type=Integer())


def _observed_column(name: str) -> ObservedColumn:
    return ObservedColumn(name=name, data_type=Integer())


def _primary_key(name: str = "table_pk", columns: tuple[str, ...] = ("id",)):
    return PrimaryKeyConstraint(columns=columns, constraint_name=name)


def _foreign_key(
    name: str = "table_customer_id_fk", local_columns: tuple[str, ...] = ("customer_id",)
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=local_columns,
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        constraint_name=name,
    )


def _create_table_action() -> CreateTable:
    return CreateTable(
        table=DesiredTable(
            qualified_name=_TARGET,
            columns=(_column("id"),),
        )
    )


def _drop_primary_key() -> DropPrimaryKey:
    return DropPrimaryKey(primary_key=_primary_key(), referencing_foreign_keys=())


def test_actionplan_truthiness_and_length():
    empty = ActionPlan(())
    non_empty = ActionPlan((DropColumn(_observed_column("legacy")),))

    assert not empty
    assert len(empty) == 0
    assert non_empty
    assert len(non_empty) == 1


def test_actionplan_rejects_non_actions():
    non_action = ColumnRenameConflict(old_name="old", new_name="new")

    with pytest.raises(TypeError, match="accepts only Action instances"):
        ActionPlan((non_action,))  # type: ignore[arg-type]


def test_plan_orders_within_a_phase_by_subject_name():
    plan = ActionPlan((AddColumn(_column("b_col")), AddColumn(_column("a_col"))))

    assert [action.subject for action in plan] == ["a_col", "b_col"]


def test_plan_ordering_is_stable_when_phase_and_subject_tie():
    first = SetProperty(name="alpha", desired_value="1", observed_value=None)
    second = SetProperty(name="alpha", desired_value="2", observed_value=None)

    assert tuple(ActionPlan((first, second))) == (first, second)


def test_plan_ordering_ignores_non_subject_fields():
    plan = ActionPlan(
        (
            SetProperty(name="b_key", desired_value="aaa", observed_value=None),
            SetProperty(name="a_key", desired_value="zzz", observed_value=None),
        )
    )

    assert [action.subject for action in plan] == ["a_key", "b_key"]


@pytest.mark.parametrize(
    "action, expected_subject",
    [
        (_create_table_action(), ""),
        (AddColumn(_column("x")), "x"),
        (DropColumn(_observed_column("x")), "x"),
        (RenameColumn("old", "new"), "old"),
        (SetProperty("prop", "1", None), "prop"),
        (UnsetProperty("prop", "old"), "prop"),
        (SetTableTag("env", "prod"), "env"),
        (UnsetTableTag("env"), "env"),
        (SetColumnTag("email", "pii", "true"), "email.pii"),
        (UnsetColumnTag("email", "pii"), "email.pii"),
        (SetColumnComment("email", "customer email", ""), "email"),
        (SetTableComment("table comment", ""), ""),
        (SetColumnNullability("email", False, True), "email"),
        (_drop_primary_key(), ""),
        (SetPrimaryKey(_primary_key()), ""),
        (DropForeignKey(_foreign_key()), "table_customer_id_fk"),
        (SetForeignKey(_foreign_key()), "customer_id"),
        (AlterClustering(("region",), ()), ""),
        (AlterColumnType("id", Long(), Integer()), "id"),
    ],
)
def test_action_subject_identifies_the_within_phase_target(action: Action, expected_subject: str):
    assert action.subject == expected_subject


@pytest.mark.parametrize(
    "action, expected_aspect",
    [
        (_create_table_action(), TableAspect.TABLE_EXISTENCE),
        (AddColumn(_column("x")), TableAspect.COLUMN_STRUCTURE),
        (DropColumn(_observed_column("x")), TableAspect.COLUMN_STRUCTURE),
        (RenameColumn("old", "new"), TableAspect.COLUMN_STRUCTURE),
        (SetProperty("prop", "1", None), TableAspect.PROPERTIES),
        (UnsetProperty("prop", "old"), TableAspect.PROPERTIES),
        (SetTableTag("env", "prod"), TableAspect.TABLE_TAGS),
        (UnsetTableTag("env"), TableAspect.TABLE_TAGS),
        (SetColumnTag("email", "pii", "true"), TableAspect.COLUMN_TAGS),
        (UnsetColumnTag("email", "pii"), TableAspect.COLUMN_TAGS),
        (SetColumnComment("email", "new", "old"), TableAspect.COLUMN_COMMENTS),
        (SetTableComment("new", "old"), TableAspect.TABLE_COMMENT),
        (SetColumnNullability("email", False, True), TableAspect.COLUMN_STRUCTURE),
        (_drop_primary_key(), TableAspect.PRIMARY_KEY),
        (SetPrimaryKey(_primary_key()), TableAspect.PRIMARY_KEY),
        (DropForeignKey(_foreign_key()), TableAspect.FOREIGN_KEYS),
        (SetForeignKey(_foreign_key()), TableAspect.FOREIGN_KEYS),
        (AlterClustering(("region",), ()), TableAspect.CLUSTERING),
        (AlterColumnType("id", Long(), Integer()), TableAspect.COLUMN_STRUCTURE),
    ],
)
def test_every_action_declares_its_table_aspect(action: Action, expected_aspect: TableAspect):
    assert action.aspect is expected_aspect


_SAMPLE_ACTIONS: list[Action] = [
    AddColumn(_column("alpha")),
    AddColumn(_column("beta")),
    DropColumn(_observed_column("gamma")),
    SetProperty("k1", "v1", None),
    SetProperty("k2", "v2", None),
    SetColumnComment("delta", "new", "old"),
    SetTableComment("new", "old"),
    SetColumnNullability("epsilon", False, True),
]


@given(st.permutations(_SAMPLE_ACTIONS))
def test_actionplan_order_is_independent_of_input_permutation(shuffled: list[Action]) -> None:
    assert tuple(ActionPlan(tuple(shuffled))) == tuple(ActionPlan(tuple(_SAMPLE_ACTIONS)))


def test_plan_full_phase_order_with_all_action_types():
    plan = ActionPlan(
        (
            SetPrimaryKey(_primary_key()),
            SetForeignKey(_foreign_key()),
            SetTableComment("new", "old"),
            AddColumn(_column("a_col")),
            SetProperty("p_set", "1", None),
            UnsetProperty("p_unset", "1"),
            SetColumnNullability("nn_col", False, True),
            DropForeignKey(_foreign_key("t_old_fk")),
            _drop_primary_key(),
            RenameColumn("old", "new"),
            DropColumn(_observed_column("d_col")),
            SetColumnTag("email", "pii", "true"),
            UnsetColumnTag("email", "old"),
            SetColumnComment("c_col", "new", "old"),
            _create_table_action(),
            SetTableTag("env", "prod"),
            UnsetTableTag("old_tag"),
            AlterClustering(("region",), ()),
            AlterColumnType("w_col", Long(), Integer()),
        )
    )

    assert [type(action) for action in plan] == [
        CreateTable,
        SetProperty,
        UnsetProperty,
        SetTableTag,
        UnsetTableTag,
        DropForeignKey,
        DropPrimaryKey,
        RenameColumn,
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


def test_plan_orders_constraint_drops_before_column_work():
    plan = ActionPlan(
        (
            DropColumn(_observed_column("customer_id")),
            _drop_primary_key(),
            DropForeignKey(_foreign_key("orders_customer_id_fk")),
            RenameColumn("old", "new"),
            AddColumn(_column("added")),
        )
    )

    assert [type(action) for action in plan] == [
        DropForeignKey,
        DropPrimaryKey,
        RenameColumn,
        AddColumn,
        DropColumn,
    ]


def test_plan_orders_property_before_type_widen_and_key_set():
    plan = ActionPlan(
        (
            SetPrimaryKey(_primary_key()),
            AlterColumnType("id", Long(), Integer()),
            SetProperty("delta.enableTypeWidening", "true", None),
        )
    )

    assert [type(action) for action in plan] == [SetProperty, AlterColumnType, SetPrimaryKey]


def test_plan_reclusters_after_add_and_before_drop():
    plan = ActionPlan(
        (
            DropColumn(_observed_column("old_region")),
            AlterClustering(("region",), ("old_region",)),
            AddColumn(_column("region")),
        )
    )

    assert [type(action) for action in plan] == [AddColumn, AlterClustering, DropColumn]


def test_enriched_actions_preserve_compiler_facing_properties():
    observed_column = _observed_column("legacy")
    primary_key = _primary_key()
    foreign_key = _foreign_key()

    assert DropColumn(observed_column).column_name == "legacy"
    assert SetProperty("prop", "new", "old").value == "new"
    assert SetColumnComment("id", "new", "old").comment == "new"
    assert SetTableComment("new", "old").comment == "new"
    assert SetColumnNullability("id", False, True).nullable is False
    assert SetPrimaryKey(primary_key).columns == ("id",)
    assert SetPrimaryKey(primary_key).constraint_name == "table_pk"
    assert DropForeignKey(foreign_key).constraint_name == "table_customer_id_fk"
    assert SetForeignKey(foreign_key).referenced_table == foreign_key.referenced_table
    assert AlterClustering(("region",), ()).columns == ("region",)
    assert AlterColumnType("id", Long(), Integer()).data_type == Long()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RenameColumn("same", "same"),
        lambda: SetProperty("prop", "same", "same"),
        lambda: SetColumnComment("id", "same", "same"),
        lambda: SetTableComment("same", "same"),
        lambda: SetColumnNullability("id", True, True),
        lambda: AlterClustering(("a", "b"), ("b", "a")),
        lambda: AlterColumnType("id", Integer(), Integer()),
    ],
)
def test_transition_actions_reject_no_difference(factory):
    with pytest.raises(ValueError, match="no difference"):
        factory()


def test_drop_foreign_key_phases_before_drop_primary_key():
    assert ActionPhase.DROP_FOREIGN_KEY < ActionPhase.DROP_PRIMARY_KEY


def test_column_rename_conflict_is_a_non_action_difference():
    difference = ColumnRenameConflict(old_name="customer_nm", new_name="customer_name")

    assert not isinstance(difference, Action)
    assert difference.aspect is TableAspect.COLUMN_STRUCTURE


def test_column_rename_conflict_rejects_no_difference():
    with pytest.raises(ValueError, match="no difference"):
        ColumnRenameConflict(old_name="same", new_name="same")
