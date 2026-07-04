import pytest

from delta_engine.domain.model import (
    ALL_ASPECTS,
    Column,
    DesiredTable,
    Integer,
    Long,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
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
    ColumnAdded,
    ColumnCommentChanged,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnRemoved,
    ColumnTagSet,
    ColumnTagUnset,
    ForeignKeyAdded,
    ForeignKeyRemoved,
    PartitioningChanged,
    PrimaryKeyAdded,
    PrimaryKeyChanged,
    PrimaryKeyRemoved,
    PropertySet,
    TableCommentChanged,
    TableDrift,
    TableMissing,
    TableTagSet,
    TableTagUnset,
    diff_table,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _desired(**overrides) -> DesiredTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))
    return DesiredTable(**{**defaults, **overrides})


def _observed(**overrides) -> ObservedTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))
    return ObservedTable(**{**defaults, **overrides})


def _foreign_key(constraint_name: str = "test_id_fk") -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "other"),
        referenced_columns=("id",),
        constraint_name=constraint_name,
    )


# ---------- top-level diff sum


def test_table_missing_carries_the_desired_table():
    # Given a desired table for a table absent from the catalog
    desired = _desired()

    # Then the missing-table variant is self-contained
    assert TableMissing(desired=desired).desired is desired


def test_missing_table_diffs_to_table_missing_carrying_desired():
    # Given no observed table
    desired = _desired()

    # When diffing against None
    diff = diff_table(desired, observed=None)

    # Then the diff is the self-contained missing-table variant
    assert diff == TableMissing(desired=desired)


def test_equal_tables_diff_to_empty_drift():
    # Given identical desired and observed definitions
    diff = diff_table(_desired(), _observed())

    # Then no changes are produced — the natural zero
    assert isinstance(diff, TableDrift)
    assert diff.changes == ()


def test_drift_carries_the_declarations_managed_aspects():
    # Given a desired table (fully managed by default)
    diff = diff_table(_desired(), _observed())

    # Then the drift is self-contained: it knows its declaration's scope
    assert isinstance(diff, TableDrift)
    assert diff.managed_aspects == ALL_ASPECTS


# ---------- column structure changes


def test_desired_only_column_produces_column_added_change():
    # Given a desired table with an extra column not in the observed table
    diff = diff_table(
        _desired(columns=(Column("id", Integer()), Column("age", Integer()))),
        _observed(),
    )

    # Then a ColumnAdded change is produced
    assert isinstance(diff, TableDrift)
    assert ColumnAdded(Column("age", Integer())) in diff.changes


def test_observed_only_column_produces_column_removed_change():
    # Given an observed table with an extra column not in the desired table
    diff = diff_table(
        _desired(),
        _observed(columns=(Column("id", Integer()), Column("stale", String()))),
    )

    # Then a ColumnRemoved change is produced
    assert isinstance(diff, TableDrift)
    assert diff.changes == (ColumnRemoved(Column("stale", String())),)


def test_type_drift_produces_column_data_type_changed():
    # Given a column whose data type differs between desired and observed
    diff = diff_table(
        _desired(columns=(Column("id", Integer()),)),
        _observed(columns=(Column("id", Long()),)),
    )

    # Then a ColumnDataTypeChanged change carries both sides
    assert isinstance(diff, TableDrift)
    assert diff.changes == (
        ColumnDataTypeChanged(column_name="id", desired_type=Integer(), observed_type=Long()),
    )


def test_type_drift_suppresses_nullability_change_but_not_comment_change():
    # Given a column where type, nullability, and comment all differ
    desired = _desired(columns=(Column("id", Integer(), nullable=False, comment="new"),))
    observed = _observed(columns=(Column("id", Long(), nullable=True, comment="old"),))

    # When diffing
    diff = diff_table(desired, observed)

    # Then the type change is present and nullability is suppressed (the column
    # must be recreated first); comment drift is independent and not suppressed
    assert isinstance(diff, TableDrift)
    assert any(isinstance(change, ColumnDataTypeChanged) for change in diff.changes)
    assert not any(isinstance(change, ColumnNullabilityChanged) for change in diff.changes)
    assert any(isinstance(change, ColumnCommentChanged) for change in diff.changes)


def test_nullability_drift_produces_column_nullability_changed():
    # Given a column whose nullability differs
    diff = diff_table(
        _desired(columns=(Column("id", Integer(), nullable=False),)),
        _observed(columns=(Column("id", Integer(), nullable=True),)),
    )

    # Then a ColumnNullabilityChanged change carries the direction
    assert isinstance(diff, TableDrift)
    assert diff.changes == (
        ColumnNullabilityChanged(column_name="id", desired_nullable=False, observed_nullable=True),
    )


# ---------- column comment changes


def test_comment_drift_on_matched_column_produces_change():
    # Given a matched column with differing comments and a desired-only column
    diff = diff_table(
        _desired(
            columns=(Column("id", Integer(), comment="pk"), Column("ghost", String(), comment="x"))
        ),
        _observed(columns=(Column("id", Integer(), comment=""),)),
    )

    # Then only the matched column produces a comment change; the ghost column's
    # comment travels inside its ColumnAdded change
    assert isinstance(diff, TableDrift)
    comment_changes = [
        change for change in diff.changes if isinstance(change, ColumnCommentChanged)
    ]
    assert comment_changes == [
        ColumnCommentChanged(column_name="id", desired_comment="pk", observed_comment="")
    ]


# ---------- column tag changes


def test_column_tag_drift_produces_set_and_unset_changes():
    # Given a column with one tag to set, one to update, and one to remove
    diff = diff_table(
        _desired(columns=(Column("id", Integer(), tags={"new": "x", "pii": "true"}),)),
        _observed(columns=(Column("id", Integer(), tags={"pii": "false", "old": "y"}),)),
    )

    # Then set changes cover added and updated tags; an unset change covers the removed tag
    assert isinstance(diff, TableDrift)
    tag_changes = {
        change for change in diff.changes if isinstance(change, (ColumnTagSet, ColumnTagUnset))
    }
    assert tag_changes == {
        ColumnTagSet(column_name="id", tag_name="new", tag_value="x"),
        ColumnTagSet(column_name="id", tag_name="pii", tag_value="true"),
        ColumnTagUnset(column_name="id", tag_name="old"),
    }


def test_added_columns_tags_produce_set_facts():
    # Given a desired-only column with tags (created by its ColumnAdded change)
    diff = diff_table(
        _desired(columns=(Column("id", Integer()), Column("new", String(), tags={"pii": "true"}))),
        _observed(),
    )

    # Then the added column's tags are changes too — ADD_COLUMN precedes SET_COLUMN_TAG
    assert isinstance(diff, TableDrift)
    assert ColumnTagSet(column_name="new", tag_name="pii", tag_value="true") in diff.changes


def test_identical_column_tags_produce_no_changes():
    # Given matched columns with identical tags
    columns = (Column("id", Integer(), tags={"pii": "true"}),)
    diff = diff_table(_desired(columns=columns), _observed(columns=columns))

    # Then no tag changes are produced
    assert isinstance(diff, TableDrift)
    assert diff.changes == ()


# ---------- table comment change


def test_table_comment_drift_produces_change_with_both_sides():
    diff = diff_table(_desired(comment="new"), _observed(comment="old"))

    assert isinstance(diff, TableDrift)
    assert diff.changes == (TableCommentChanged(desired_comment="new", observed_comment="old"),)


# ---------- property changes (declared-projection)


def test_declared_property_drift_produces_property_set_changes():
    # Given one declared property missing from the catalog and one with a stale value
    diff = diff_table(
        _desired(properties={"a": "1", "b": "2"}),
        _observed(properties={"b": "9"}),
    )

    # Then each declared difference is one change
    assert isinstance(diff, TableDrift)
    assert set(diff.changes) == {
        PropertySet(name="a", desired_value="1"),
        PropertySet(name="b", desired_value="2"),
    }


def test_declared_property_matching_catalog_produces_no_change():
    # Given a declared property whose catalog value already matches
    diff = diff_table(
        _desired(properties={"a": "1"}),
        _observed(properties={"a": "1"}),
    )

    # Then no change is produced — the property sync is idempotent
    assert isinstance(diff, TableDrift)
    assert diff.changes == ()


def test_observed_only_property_is_not_drift():
    # Given a catalog property the declaration does not own
    # (e.g. delta.columnMapping.mode written by a previous full sync)
    diff = diff_table(
        _desired(properties={}),
        _observed(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then no change is produced — declared-projection semantics
    assert isinstance(diff, TableDrift)
    assert diff.changes == ()


# ---------- table tag changes (full-state)


def test_table_tag_drift_produces_set_and_unset_changes():
    # Given one declared tag missing from the catalog and one observed-only tag
    diff = diff_table(
        _desired(tags={"env": "prod"}),
        _observed(tags={"stale": "yes"}),
    )

    # Then the declared tag is set and the undeclared tag is unset — full-state
    assert isinstance(diff, TableDrift)
    assert set(diff.changes) == {
        TableTagSet(name="env", value="prod"),
        TableTagUnset(name="stale"),
    }


# ---------- partitioning change


def test_partitioning_drift_produces_change_with_both_sides():
    diff = diff_table(_desired(partitioned_by=("id",)), _observed())

    assert isinstance(diff, TableDrift)
    assert diff.changes == (
        PartitioningChanged(desired_partitioning=("id",), observed_partitioning=()),
    )


# ---------- primary key changes


def test_desired_only_primary_key_produces_added_change():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")
    diff = diff_table(
        _desired(columns=(Column("id", Integer(), nullable=False),), primary_key=pk),
        _observed(columns=(Column("id", Integer(), nullable=False),)),
    )

    assert isinstance(diff, TableDrift)
    assert diff.changes == (PrimaryKeyAdded(primary_key=pk),)


def test_equal_primary_keys_by_column_set_produce_no_change():
    # Given the same PK column set under different orders and names
    desired_pk = PrimaryKeyConstraint(columns=("a", "b"), constraint_name="test_pk")
    observed_pk = PrimaryKeyConstraint(columns=("b", "a"), constraint_name="other_name")
    columns = (Column("a", Integer(), nullable=False), Column("b", Integer(), nullable=False))

    diff = diff_table(
        _desired(columns=columns, primary_key=desired_pk),
        _observed(columns=columns, primary_key=observed_pk),
    )

    # Then identity is column-set equality — no change
    assert isinstance(diff, TableDrift)
    assert diff.changes == ()


# ---------- foreign key changes


def test_desired_only_foreign_key_produces_added_change():
    fk = _foreign_key()
    diff = diff_table(_desired(foreign_keys=(fk,)), _observed())

    assert isinstance(diff, TableDrift)
    assert diff.changes == (ForeignKeyAdded(constraint=fk),)


def test_equal_foreign_keys_by_signature_produce_no_change():
    # Given the same FK relationship under different constraint names
    diff = diff_table(
        _desired(foreign_keys=(_foreign_key("engine_name"),)),
        _observed(foreign_keys=(_foreign_key("external_name"),)),
    )

    # Then identity is the content signature — no change, sync stays idempotent
    assert isinstance(diff, TableDrift)
    assert diff.changes == ()


# ---------- no-difference changes are unrepresentable


def test_column_data_type_changed_rejects_equal_types():
    with pytest.raises(ValueError, match="no difference"):
        ColumnDataTypeChanged(column_name="id", desired_type=Integer(), observed_type=Integer())


def test_column_nullability_changed_rejects_equal_flags():
    with pytest.raises(ValueError, match="no difference"):
        ColumnNullabilityChanged(column_name="id", desired_nullable=True, observed_nullable=True)


def test_column_comment_changed_rejects_equal_comments():
    with pytest.raises(ValueError, match="no difference"):
        ColumnCommentChanged(column_name="id", desired_comment="same", observed_comment="same")


def test_table_comment_changed_rejects_equal_comments():
    with pytest.raises(ValueError, match="no difference"):
        TableCommentChanged(desired_comment="same", observed_comment="same")


def test_partitioning_changed_rejects_equal_specs():
    with pytest.raises(ValueError, match="no difference"):
        PartitioningChanged(desired_partitioning=("ds",), observed_partitioning=("ds",))


def test_primary_key_changed_rejects_equal_column_sets():
    pk_a = PrimaryKeyConstraint(columns=("a", "b"), constraint_name="x")
    pk_b = PrimaryKeyConstraint(columns=("b", "a"), constraint_name="y")

    with pytest.raises(ValueError, match="no difference"):
        PrimaryKeyChanged(desired_primary_key=pk_a, observed_primary_key=pk_b)


# ---------- change lowering: actions()


def test_column_added_produces_add_column_only():
    # Given an added column with tags — its tags arrive as separate ColumnTagSet changes
    column = Column("age", Integer(), tags={"pii": "false"})

    assert ColumnAdded(column=column).actions() == (AddColumn(column=column),)


def test_column_removed_produces_drop_column():
    assert ColumnRemoved(column=Column("stale", Integer())).actions() == (DropColumn("stale"),)


def test_column_data_type_changed_produces_no_actions():
    # Given a type change — no in-place remedy exists; validation blocks it
    change = ColumnDataTypeChanged(column_name="id", desired_type=Integer(), observed_type=Long())

    assert change.actions() == ()


def test_column_nullability_changed_produces_set_column_nullability():
    change = ColumnNullabilityChanged(
        column_name="id", desired_nullable=True, observed_nullable=False
    )

    assert change.actions() == (SetColumnNullability(column_name="id", nullable=True),)


def test_column_comment_changed_produces_set_column_comment():
    change = ColumnCommentChanged(column_name="id", desired_comment="pk", observed_comment="")

    assert change.actions() == (SetColumnComment("id", "pk"),)


def test_column_tag_set_produces_set_column_tag():
    change = ColumnTagSet(column_name="id", tag_name="pii", tag_value="true")

    assert change.actions() == (SetColumnTag(column_name="id", name="pii", value="true"),)


def test_column_tag_unset_produces_unset_column_tag():
    change = ColumnTagUnset(column_name="id", tag_name="old")

    assert change.actions() == (UnsetColumnTag(column_name="id", name="old"),)


def test_table_comment_changed_produces_set_table_comment():
    change = TableCommentChanged(desired_comment="new", observed_comment="old")

    assert change.actions() == (SetTableComment(comment="new"),)


def test_property_set_produces_set_property():
    change = PropertySet(name="delta.appendOnly", desired_value="true")

    assert change.actions() == (SetProperty(name="delta.appendOnly", value="true"),)


def test_table_tag_set_produces_set_table_tag():
    assert TableTagSet(name="env", value="prod").actions() == (
        SetTableTag(name="env", value="prod"),
    )


def test_table_tag_unset_produces_unset_table_tag():
    assert TableTagUnset(name="stale").actions() == (UnsetTableTag(name="stale"),)


def test_partitioning_changed_produces_no_actions():
    # Given a partitioning change — no in-place remedy exists; validation blocks it
    change = PartitioningChanged(desired_partitioning=("ds",), observed_partitioning=())

    assert change.actions() == ()


def test_primary_key_added_produces_set_primary_key():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")

    assert PrimaryKeyAdded(primary_key=pk).actions() == (
        SetPrimaryKey(columns=("id",), constraint_name="test_pk"),
    )


def test_primary_key_removed_produces_drop_primary_key():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="legacy_pk")

    assert PrimaryKeyRemoved(observed_primary_key=pk).actions() == (DropPrimaryKey(),)


def test_primary_key_changed_produces_drop_then_set():
    # Given a changed primary key (column set differs)
    desired_pk = PrimaryKeyConstraint(columns=("a",), constraint_name="test_pk")
    observed_pk = PrimaryKeyConstraint(columns=("b",), constraint_name="test_pk")
    change = PrimaryKeyChanged(desired_primary_key=desired_pk, observed_primary_key=observed_pk)

    # When the actions are sorted by ActionPlan (drop runs before set)
    plan = ActionPlan(change.actions())

    # Then the plan contains DropPrimaryKey followed by SetPrimaryKey
    assert plan.actions == (
        DropPrimaryKey(),
        SetPrimaryKey(columns=("a",), constraint_name="test_pk"),
    )


def test_foreign_key_added_produces_set_foreign_key():
    fk = _foreign_key()

    assert ForeignKeyAdded(constraint=fk).actions() == (
        SetForeignKey(
            local_columns=("id",),
            referenced_table=QualifiedName("dev", "silver", "other"),
            referenced_columns=("id",),
            constraint_name="test_id_fk",
        ),
    )


def test_foreign_key_removed_produces_drop_foreign_key():
    fk = _foreign_key("stale_fk")

    assert ForeignKeyRemoved(constraint=fk).actions() == (
        DropForeignKey(constraint_name="stale_fk"),
    )
