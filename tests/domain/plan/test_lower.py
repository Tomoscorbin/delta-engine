from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan.actions import (
    AddColumn,
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
    UnsetTableTag,
)
from delta_engine.domain.plan.diff import (
    Added,
    Changed,
    ColumnChanged,
    KeyValue,
    Removed,
    TableDrift,
    TableMissing,
)
from delta_engine.domain.plan.lower import compute_plan, lower_diff

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _desired(**overrides) -> DesiredTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))
    return DesiredTable(**{**defaults, **overrides})


# ---------- TableMissing ----------


def test_missing_table_lowers_to_create_plus_tag_and_fk_follow_ups():
    # Given a missing table whose declaration carries tags and a foreign key
    foreign_key = ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "other"),
        referenced_columns=("id",),
        constraint_name="test_id_fk",
    )
    desired = _desired(
        columns=(Column("id", Integer(), tags={"pii": "false"}),),
        tags={"env": "prod"},
        foreign_keys=(foreign_key,),
    )

    # When lowering the missing-table diff
    plan = lower_diff(TableMissing(desired=desired))

    # Then CREATE TABLE carries the declaration, and tags/FKs (which cannot be
    # declared inline in CREATE TABLE) follow as separate actions.
    # Note: Column.tags is a dict so Column is unhashable; use list comparison
    # rather than set() — consistent with test_differ.py for the same scenario.
    assert len(plan.actions) == 4
    assert any(isinstance(a, CreateTable) and a.table == desired for a in plan.actions)
    non_create = [a for a in plan.actions if not isinstance(a, CreateTable)]
    assert set(non_create) == {
        SetTableTag(name="env", value="prod"),
        SetColumnTag(column_name="id", name="pii", value="false"),
        SetForeignKey(
            local_columns=("id",),
            referenced_table=QualifiedName("dev", "silver", "other"),
            referenced_columns=("id",),
            constraint_name="test_id_fk",
        ),
    }


# ---------- column drift ----------


def test_added_column_lowers_to_add_column_plus_its_tags():
    column = Column("age", Integer(), tags={"pii": "false"})

    plan = lower_diff(TableDrift(columns=(Added(column),)))

    # Note: Column.tags is a dict so AddColumn(column=...) is unhashable; use
    # isinstance check for AddColumn and set() for the tag-only actions.
    assert len(plan.actions) == 2
    assert any(isinstance(a, AddColumn) and a.column == column for a in plan.actions)
    tag_actions = [a for a in plan.actions if isinstance(a, SetColumnTag)]
    assert tag_actions == [SetColumnTag(column_name="age", name="pii", value="false")]


def test_removed_column_lowers_to_drop_column_only():
    # Given an observed-only column carrying tags
    column = Column("stale", Integer(), tags={"old": "y"})

    plan = lower_diff(TableDrift(columns=(Removed(column),)))

    # Then dropping the column is the whole response — its tags die with it
    assert plan.actions == (DropColumn("stale"),)


def test_column_changed_sub_facts_lower_independently():
    entry = ColumnChanged(
        column_name="id",
        nullability=Changed(desired=True, observed=False),
        comment=Changed(desired="pk", observed=""),
        tags=(
            Added(KeyValue("new", "x")),
            Changed(desired=KeyValue("pii", "true"), observed=KeyValue("pii", "false")),
            Removed(KeyValue("old", "y")),
        ),
    )

    plan = lower_diff(TableDrift(columns=(entry,)))

    assert set(plan.actions) == {
        SetColumnNullability(column_name="id", nullable=True),
        SetColumnComment("id", "pk"),
        SetColumnTag(column_name="id", name="new", value="x"),
        SetColumnTag(column_name="id", name="pii", value="true"),
        UnsetColumnTag(column_name="id", name="old"),
    }


def test_type_drift_lowers_to_nothing():
    # Given a fact validation has already rejected — type migrations are unsupported
    entry = ColumnChanged(
        column_name="id", data_type=Changed(desired=Integer(), observed=Integer())
    )

    plan = lower_diff(TableDrift(columns=(entry,)))

    # Then there is no action to translate it into
    assert plan.actions == ()


# ---------- table-level dimensions ----------


def test_table_comment_change_lowers_to_set_table_comment():
    plan = lower_diff(TableDrift(table_comment=Changed(desired="new", observed="old")))

    assert plan.actions == (SetTableComment(comment="new"),)


def test_property_entries_lower_with_declared_subset_semantics():
    # Given properties drifting in all three ways
    plan = lower_diff(
        TableDrift(
            properties=(
                Added(KeyValue("a", "1")),
                Changed(desired=KeyValue("b", "2"), observed=KeyValue("b", "9")),
                Removed(KeyValue("c", "3")),
            )
        )
    )

    # Then Added/Changed are set and Removed is ignored — the engine only
    # manages keys the user declared; observed-only properties are never unset
    assert set(plan.actions) == {
        SetProperty(name="a", value="1"),
        SetProperty(name="b", value="2"),
    }


def test_table_tag_entries_lower_with_full_state_semantics():
    plan = lower_diff(
        TableDrift(
            table_tags=(
                Added(KeyValue("env", "prod")),
                Removed(KeyValue("stale", "yes")),
            )
        )
    )

    assert set(plan.actions) == {
        SetTableTag(name="env", value="prod"),
        UnsetTableTag(name="stale"),
    }


def test_partitioning_drift_lowers_to_nothing():
    plan = lower_diff(TableDrift(partitioning=Changed(desired=("ds",), observed=())))

    assert plan.actions == ()


def test_added_primary_key_lowers_to_set_primary_key():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")

    plan = lower_diff(TableDrift(primary_key=Added(pk)))

    assert plan.actions == (SetPrimaryKey(columns=("id",), constraint_name="test_pk"),)


def test_removed_primary_key_lowers_to_drop_primary_key():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="legacy_pk")

    plan = lower_diff(TableDrift(primary_key=Removed(pk)))

    assert plan.actions == (DropPrimaryKey(),)


def test_changed_primary_key_lowers_to_drop_then_set():
    desired_pk = PrimaryKeyConstraint(columns=("a",), constraint_name="test_pk")
    observed_pk = PrimaryKeyConstraint(columns=("b",), constraint_name="test_pk")

    plan = lower_diff(TableDrift(primary_key=Changed(desired=desired_pk, observed=observed_pk)))

    # ActionPlan orders by phase: the drop precedes the set
    assert plan.actions == (
        DropPrimaryKey(),
        SetPrimaryKey(columns=("a",), constraint_name="test_pk"),
    )


def test_foreign_key_entries_lower_to_set_and_drop():
    foreign_key = ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "other"),
        referenced_columns=("id",),
        constraint_name="test_id_fk",
    )
    stale = ForeignKeyConstraint(
        local_columns=("other_id",),
        referenced_table=QualifiedName("dev", "silver", "third"),
        referenced_columns=("id",),
        constraint_name="stale_fk",
    )

    plan = lower_diff(TableDrift(foreign_keys=(Added(foreign_key), Removed(stale))))

    assert set(plan.actions) == {
        SetForeignKey(
            local_columns=("id",),
            referenced_table=QualifiedName("dev", "silver", "other"),
            referenced_columns=("id",),
            constraint_name="test_id_fk",
        ),
        DropForeignKey(constraint_name="stale_fk"),
    }


# ---------- composition ----------


def test_compute_plan_is_diff_then_lower():
    # Given equal desired and observed states
    from delta_engine.domain.model import ObservedTable

    desired = _desired()
    observed = ObservedTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))

    # Then the composition yields an empty plan
    assert compute_plan(desired, observed).actions == ()
    # And a missing table yields a creation plan
    assert compute_plan(desired, None).actions == (CreateTable(desired),)
