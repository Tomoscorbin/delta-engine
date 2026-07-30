import inspect

from hypothesis import given, strategies as st
import pytest

from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ForeignKeyConstraint,
    Integer,
    Long,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
    TableAspect,
    TableFeature,
)
import delta_engine.domain.plan.actions as actions_module
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
    EnableTableFeature,
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
from delta_engine.domain.plan.unresolvable import ColumnRenameConflict, PartitioningChanged

_TARGET = QualifiedName("cat", "sch", "table")


def _column(name: str) -> DesiredColumn:
    return DesiredColumn(name=name, data_type=Integer())


def _observed_column(name: str) -> ObservedColumn:
    return ObservedColumn(name=name, data_type=Integer())


def _plan(*plan_actions: Action) -> ActionPlan:
    return ActionPlan(target=_TARGET, actions=plan_actions)


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
    return DropPrimaryKey(primary_key=_primary_key())


def _concrete_action_types() -> list[type[Action]]:
    """
    Return every concrete Action subclass exposed by the actions module.

    This uses the module namespace rather than Action.__subclasses__() because
    dataclass(slots=True) can leave stale pre-slots class objects there.
    """
    return [
        obj
        for _, obj in inspect.getmembers(actions_module, inspect.isclass)
        if issubclass(obj, Action)
        and obj is not Action
        and not getattr(obj, "__abstractmethods__", False)
    ]


def test_actionplan_truthiness_and_length():
    empty = _plan()
    non_empty = _plan(DropColumn(_observed_column("legacy")))

    assert not empty
    assert len(empty) == 0
    assert non_empty
    assert len(non_empty) == 1


def test_actionplan_rejects_create_table_for_a_different_target():
    with pytest.raises(ValueError, match="CreateTable target does not match ActionPlan target"):
        ActionPlan(
            target=QualifiedName("cat", "sch", "other"),
            actions=(_create_table_action(),),
        )


def test_plan_orders_within_a_phase_by_subject_name():
    plan = _plan(AddColumn(_column("b_col")), AddColumn(_column("a_col")))

    assert [action.subject for action in plan] == ["a_col", "b_col"]


def test_plan_ordering_is_stable_when_phase_and_subject_tie():
    first = SetProperty(name="alpha", desired_value="1", observed_value=None)
    second = SetProperty(name="alpha", desired_value="2", observed_value=None)

    assert tuple(_plan(first, second)) == (first, second)


def test_plan_ordering_ignores_non_subject_fields():
    plan = _plan(
        SetProperty(name="b_key", desired_value="aaa", observed_value=None),
        SetProperty(name="a_key", desired_value="zzz", observed_value=None),
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
        (SetTableTag("env", "prod", None), "env"),
        (UnsetTableTag("env"), "env"),
        (SetColumnTag("email", "pii", "true", None), "email.pii"),
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
        (SetTableTag("env", "prod", None), TableAspect.TABLE_TAGS),
        (UnsetTableTag("env"), TableAspect.TABLE_TAGS),
        (SetColumnTag("email", "pii", "true", None), TableAspect.COLUMN_TAGS),
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
    assert tuple(_plan(*shuffled)) == tuple(_plan(*_SAMPLE_ACTIONS))


def test_plan_full_phase_order_with_all_action_types():
    # Given one action of every type, declared in a jumbled order
    plan = _plan(
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
        SetColumnTag("email", "pii", "true", None),
        UnsetColumnTag("email", "old"),
        SetColumnComment("c_col", "new", "old"),
        _create_table_action(),
        SetTableTag("env", "prod", None),
        UnsetTableTag("old_tag"),
        AlterClustering(("region",), ()),
        AlterColumnType("w_col", Long(), Integer()),
    )

    # Then the plan orders them by canonical phase
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
        SetColumnTag,
        UnsetColumnTag,
        DropColumn,
        SetColumnComment,
        SetTableComment,
        SetColumnNullability,
        SetPrimaryKey,
        SetForeignKey,
    ]


def test_plan_orders_constraint_drops_before_column_work():
    plan = _plan(
        DropColumn(_observed_column("customer_id")),
        _drop_primary_key(),
        DropForeignKey(_foreign_key("orders_customer_id_fk")),
        RenameColumn("old", "new"),
        AddColumn(_column("added")),
    )

    assert [type(action) for action in plan] == [
        DropForeignKey,
        DropPrimaryKey,
        RenameColumn,
        AddColumn,
        DropColumn,
    ]


def test_plan_orders_property_before_type_widen_and_key_set():
    plan = _plan(
        SetPrimaryKey(_primary_key()),
        AlterColumnType("id", Long(), Integer()),
        SetProperty("delta.enableTypeWidening", "true", None),
    )

    assert [type(action) for action in plan] == [SetProperty, AlterColumnType, SetPrimaryKey]


def test_plan_reclusters_after_add_and_before_drop():
    plan = _plan(
        DropColumn(_observed_column("old_region")),
        AlterClustering(("region",), ("old_region",)),
        AddColumn(_column("region")),
    )

    assert [type(action) for action in plan] == [AddColumn, AlterClustering, DropColumn]


def test_plan_unsets_column_tags_before_dropping_columns():
    plan = _plan(
        DropColumn(_observed_column("legacy")),
        UnsetColumnTag("legacy", "governed"),
    )

    assert [type(action) for action in plan] == [UnsetColumnTag, DropColumn]


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RenameColumn("same", "same"),
        lambda: SetProperty("prop", "same", "same"),
        lambda: SetTableTag("tag", "same", "same"),
        lambda: SetColumnTag("column", "tag", "same", "same"),
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


def test_column_rename_conflict_is_unresolvable_not_an_action():
    unresolvable = ColumnRenameConflict(old_name="customer_nm", new_name="customer_name")

    assert not isinstance(unresolvable, Action)
    assert unresolvable.aspect is TableAspect.COLUMN_STRUCTURE


def test_column_rename_conflict_rejects_no_difference():
    with pytest.raises(ValueError, match="no difference"):
        ColumnRenameConflict(old_name="same", new_name="same")


def test_partitioning_changed_rejects_no_difference():
    with pytest.raises(ValueError, match="no difference"):
        PartitioningChanged(desired_partitioning=("ds",), observed_partitioning=("ds",))


def test_tag_aspects_belong_to_exactly_the_four_tag_actions():
    # The streaming-table eligibility check admits only the tag aspects, so the
    # actions carrying them are the complete set of statements the engine can
    # aim at a streaming table. Growing this set is a policy decision, not a
    # side effect: an action added with a tag aspect flows to streaming
    # tables automatically.
    tag_aspects = {TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}
    tag_actions = {
        action_type.__name__
        for action_type in _concrete_action_types()
        if action_type.aspect in tag_aspects
    }

    assert tag_actions == {"SetTableTag", "UnsetTableTag", "SetColumnTag", "UnsetColumnTag"}


def test_enable_table_feature_declares_aspect_phase_and_subject():
    # Given a table-feature enablement action
    action = EnableTableFeature(feature=TableFeature.TIMESTAMP_NTZ)

    # When its planning metadata is inspected
    metadata = (action.aspect, action.phase, action.subject)

    # Then it belongs to column structure and has a dedicated execution phase
    assert metadata == (
        TableAspect.COLUMN_STRUCTURE,
        ActionPhase.ENABLE_TABLE_FEATURE,
        "timestampNtz",
    )


def test_enable_table_feature_phases_before_every_dependent_column_phase():
    # Given the create, feature, property, and dependent column phases
    dependent_phases = (
        ActionPhase.SET_PROPERTY,
        ActionPhase.RENAME_COLUMN,
        ActionPhase.ADD_COLUMN,
        ActionPhase.ALTER_COLUMN_TYPE,
    )

    # When comparing every dependent phase with feature enablement
    feature_precedes_dependencies = all(
        ActionPhase.ENABLE_TABLE_FEATURE < phase for phase in dependent_phases
    )

    # Then creation precedes feature enablement, which precedes every dependency
    assert ActionPhase.CREATE_TABLE < ActionPhase.ENABLE_TABLE_FEATURE
    assert feature_precedes_dependencies


def test_action_plan_orders_feature_enable_before_column_actions():
    # Given dependent actions supplied in the wrong execution order
    actions = (
        AddColumn(column=_column("seen_at")),
        EnableTableFeature(feature=TableFeature.TIMESTAMP_NTZ),
    )

    # When constructing an action plan
    plan = ActionPlan(
        target=_TARGET,
        actions=actions,
    )

    # Then feature enablement is ordered before the dependent column addition
    assert [type(action) for action in plan.actions] == [EnableTableFeature, AddColumn]


def test_plan_orders_subjects_by_lowercased_key_not_ascii_order():
    # Deterministic ordering must not depend on subject casing: 'Beta' sorts
    # after 'alpha' by lowercased key, not before it by raw ASCII ('B' < 'a').
    plan = ActionPlan(
        target=QualifiedName("cat", "sch", "t"),
        actions=(
            SetColumnComment(column_name="Beta", desired_comment="x", observed_comment=""),
            SetColumnComment(column_name="alpha", desired_comment="x", observed_comment=""),
        ),
    )

    assert [str(action.subject) for action in plan] == ["alpha", "Beta"]


def test_case_only_rename_carries_no_difference_even_from_raw_strings() -> None:
    with pytest.raises(ValueError, match="carries no difference"):
        RenameColumn(old_name="requestid", new_name="REQUESTID")


def test_case_variant_clustering_carries_no_difference_even_from_raw_strings() -> None:
    with pytest.raises(ValueError, match="carries no difference"):
        AlterClustering(desired_clustering=("A", "b"), observed_clustering=("a", "B"))
