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

_TARGET = QualifiedName("cat", "sch", "table")


def _column(name: str) -> DesiredColumn:
    return DesiredColumn(name=name, data_type=Integer())


def _observed_column(name: str) -> ObservedColumn:
    return ObservedColumn(name=name, data_type=Integer())


def _plan(*plan_actions: Action) -> ActionPlan:
    return ActionPlan(target=_TARGET, actions=plan_actions)


def _primary_key(name: str | None = "table_pk", columns: tuple[str, ...] = ("id",)):
    return PrimaryKeyConstraint(columns=columns, name=name)


def _foreign_key(
    name: str | None = "table_customer_id_fk",
    local_columns: tuple[str, ...] = ("customer_id",),
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=local_columns,
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        name=name,
    )


def _create_table_action() -> CreateTable:
    return CreateTable(
        table=DesiredTable(
            qualified_name=_TARGET,
            columns=(_column("id"),),
        )
    )


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
    # Given an empty plan and a one-action plan
    empty = _plan()
    non_empty = _plan(DropColumn(_observed_column("legacy")))

    # Then truthiness and length report whether there is anything to execute
    assert not empty
    assert len(empty) == 0
    assert non_empty
    assert len(non_empty) == 1


def test_actionplan_rejects_create_table_for_a_different_target():
    # Given a CreateTable action for one table inside a plan targeting another
    # Then construction fails
    with pytest.raises(ValueError):
        ActionPlan(
            target=QualifiedName("cat", "sch", "other"),
            actions=(_create_table_action(),),
        )


def test_plan_orders_within_a_phase_by_subject_name():
    # Given two same-phase actions declared out of subject order
    plan = _plan(AddColumn(_column("b_col")), AddColumn(_column("a_col")))

    # Then the plan orders them by subject
    assert [action.subject for action in plan] == ["a_col", "b_col"]


def test_plan_ordering_is_stable_when_phase_and_subject_tie():
    # Given two actions that tie on phase and subject
    first = SetProperty(name="alpha", desired_value="1", observed_value=None)
    second = SetProperty(name="alpha", desired_value="2", observed_value=None)

    # Then the supplied order is preserved
    assert tuple(_plan(first, second)) == (first, second)


def test_plan_ordering_ignores_non_subject_fields():
    # Given two same-phase actions whose non-subject fields sort opposite to their subjects
    plan = _plan(
        SetProperty(name="b_key", desired_value="aaa", observed_value=None),
        SetProperty(name="a_key", desired_value="zzz", observed_value=None),
    )

    # Then only the subject drives the order
    assert [action.subject for action in plan] == ["a_key", "b_key"]


@pytest.mark.parametrize(
    "action, expected_subject",
    [
        (_create_table_action(), ""),
        (EnableTableFeature(TableFeature.TIMESTAMP_NTZ), "timestampNtz"),
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
        (DropPrimaryKey("table_pk"), "table_pk"),
        (SetPrimaryKey(_primary_key()), "id"),
        (
            SetPrimaryKey(_primary_key(None, columns=("tenant_id", "order_id"))),
            "order_id,tenant_id",
        ),
        (DropForeignKey("table_customer_id_fk"), "table_customer_id_fk"),
        (SetForeignKey(_foreign_key()), "customer_id"),
        (AlterClustering(("region",), ()), ""),
        (AlterColumnType("id", Long(), Integer()), "id"),
    ],
)
def test_action_subject_identifies_the_within_phase_target(action: Action, expected_subject: str):
    # Then each action names the identifier it targets within its phase
    # (an unnamed key falls back to its column set)
    assert action.subject == expected_subject


@pytest.mark.parametrize(
    "action, expected_aspect",
    [
        (_create_table_action(), TableAspect.TABLE_EXISTENCE),
        (EnableTableFeature(TableFeature.TIMESTAMP_NTZ), TableAspect.COLUMN_STRUCTURE),
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
        (DropPrimaryKey("table_pk"), TableAspect.PRIMARY_KEY),
        (SetPrimaryKey(_primary_key()), TableAspect.PRIMARY_KEY),
        (DropForeignKey("table_customer_id_fk"), TableAspect.FOREIGN_KEYS),
        (SetForeignKey(_foreign_key()), TableAspect.FOREIGN_KEYS),
        (AlterClustering(("region",), ()), TableAspect.CLUSTERING),
        (AlterColumnType("id", Long(), Integer()), TableAspect.COLUMN_STRUCTURE),
    ],
)
def test_every_action_declares_its_table_aspect(action: Action, expected_aspect: TableAspect):
    # Then each action states which table aspect it manages, so scope-based
    # reconciliation admits exactly the right actions
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
    # Then a plan orders the same actions identically whatever order they arrive in
    assert tuple(_plan(*shuffled)) == tuple(_plan(*_SAMPLE_ACTIONS))


def test_plan_orders_constraint_drops_before_column_work():
    # Given constraint drops mixed with column changes
    plan = _plan(
        DropColumn(_observed_column("customer_id")),
        DropPrimaryKey("table_pk"),
        DropForeignKey("orders_customer_id_fk"),
        RenameColumn("old", "new"),
        AddColumn(_column("added")),
    )

    # Then constraints are dropped before the columns they may depend on change
    assert [type(action) for action in plan] == [
        DropForeignKey,
        DropPrimaryKey,
        RenameColumn,
        AddColumn,
        DropColumn,
    ]


def test_plan_orders_property_before_type_widen_and_key_set():
    # Given a type widen that depends on a property, and a key set on the column
    plan = _plan(
        SetPrimaryKey(_primary_key()),
        AlterColumnType("id", Long(), Integer()),
        SetProperty("delta.enableTypeWidening", "true", None),
    )

    # Then the property lands first and the key lands last
    assert [type(action) for action in plan] == [SetProperty, AlterColumnType, SetPrimaryKey]


def test_plan_reclusters_after_add_and_before_drop():
    # Given a clustering change whose new key is added and old key is dropped
    plan = _plan(
        DropColumn(_observed_column("old_region")),
        AlterClustering(("region",), ("old_region",)),
        AddColumn(_column("region")),
    )

    # Then reclustering runs after the add and before the drop
    assert [type(action) for action in plan] == [AddColumn, AlterClustering, DropColumn]


def test_plan_unsets_column_tags_before_dropping_columns():
    # Given a column drop and a tag unset on the same column
    plan = _plan(
        DropColumn(_observed_column("legacy")),
        UnsetColumnTag("legacy", "governed"),
    )

    # Then the tag is unset before its column disappears
    assert [type(action) for action in plan] == [UnsetColumnTag, DropColumn]


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RenameColumn("same", "same"),
        lambda: RenameColumn("requestid", "REQUESTID"),
        lambda: SetProperty("prop", "same", "same"),
        lambda: SetTableTag("tag", "same", "same"),
        lambda: SetColumnTag("column", "tag", "same", "same"),
        lambda: SetColumnComment("id", "same", "same"),
        lambda: SetTableComment("same", "same"),
        lambda: SetColumnNullability("id", True, True),
        lambda: AlterClustering(("a", "b"), ("b", "a")),
        lambda: AlterClustering(("A", "b"), ("a", "B")),
        lambda: AlterColumnType("id", Integer(), Integer()),
    ],
    ids=[
        "rename",
        "case-only-rename",
        "property",
        "table-tag",
        "column-tag",
        "column-comment",
        "table-comment",
        "nullability",
        "reordered-clustering",
        "case-variant-clustering",
        "column-type",
    ],
)
def test_transition_actions_reject_no_difference(factory):
    # When a transition action carries no difference — including differences
    # that vanish under case-insensitive identity — then construction fails
    with pytest.raises(ValueError):
        factory()


def test_tag_aspects_belong_to_exactly_the_four_tag_actions():
    # The tag-only scope admits exactly these aspects. Growing the action set is
    # a policy decision, not a side effect: an action added with a tag aspect
    # flows into tag reconciliation automatically.
    tag_aspects = {TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}
    tag_actions = {
        action_type.__name__
        for action_type in _concrete_action_types()
        if action_type.aspect in tag_aspects
    }

    assert tag_actions == {"SetTableTag", "UnsetTableTag", "SetColumnTag", "UnsetColumnTag"}


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
    # Given subjects whose raw ASCII order disagrees with their lowercased
    # order: 'Beta' sorts after 'alpha' by lowercased key, not before it by
    # raw ASCII ('B' < 'a')
    plan = ActionPlan(
        target=QualifiedName("cat", "sch", "t"),
        actions=(
            SetColumnComment(column_name="Beta", desired_comment="x", observed_comment=""),
            SetColumnComment(column_name="alpha", desired_comment="x", observed_comment=""),
        ),
    )

    # Then deterministic ordering does not depend on subject casing
    assert [str(action.subject) for action in plan] == ["alpha", "Beta"]
