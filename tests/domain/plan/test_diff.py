import pytest

from delta_engine.domain.model import (
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
    Added,
    Changed,
    ColumnAdded,
    ColumnCommentChanged,
    ColumnCommentsDimension,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnRemoved,
    ColumnStructureDimension,
    ColumnTagsChanged,
    ColumnTagsDimension,
    ForeignKeysDimension,
    KeyValue,
    PartitioningDimension,
    PrimaryKeyDimension,
    PropertiesDimension,
    Removed,
    TableCommentDimension,
    TableDrift,
    TableMissing,
    TableTagsDimension,
    diff_table,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def test_table_drift_defaults_to_no_differences():
    # Given a drift built with no arguments
    drift = TableDrift()

    # Then there are no dimensions
    assert drift.dimensions == ()


def test_table_missing_carries_the_desired_table():
    # Given a desired table for a table absent from the catalog
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))

    # Then the missing-table variant is self-contained
    assert TableMissing(desired=desired).desired is desired


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

    # Then no dimensions are produced
    assert isinstance(diff, TableDrift)
    assert diff.dimensions == ()


def test_desired_only_column_produces_columns_dimension_with_added_entry():
    # Given a desired table with an extra column not in the observed table
    # When diffing
    diff = diff_table(
        _desired(columns=(Column("id", Integer()), Column("age", Integer()))),
        _observed(),
    )

    # Then a ColumnStructureDimension with an Added entry is produced
    assert isinstance(diff, TableDrift)
    assert len(diff.dimensions) == 1
    dim = diff.dimensions[0]
    assert isinstance(dim, ColumnStructureDimension)
    assert dim.entries == (ColumnAdded(Column("age", Integer())),)


def test_observed_only_column_produces_columns_dimension_with_removed_entry():
    diff = diff_table(
        _desired(),
        _observed(columns=(Column("id", Integer()), Column("stale", String()))),
    )

    assert isinstance(diff, TableDrift)
    dim = diff.dimensions[0]
    assert isinstance(dim, ColumnStructureDimension)
    assert dim.entries == (ColumnRemoved(Column("stale", String())),)


def test_type_drift_produces_columns_dimension_with_data_type_changed_entry():
    diff = diff_table(
        _desired(columns=(Column("id", Integer()),)),
        _observed(columns=(Column("id", Long()),)),
    )

    assert isinstance(diff, TableDrift)
    dim = diff.dimensions[0]
    assert isinstance(dim, ColumnStructureDimension)
    assert dim.entries == (
        ColumnDataTypeChanged(
            column_name="id",
            change=Changed(desired=Integer(), observed=Long()),
        ),
    )


def test_table_comment_drift_produces_table_comment_dimension():
    diff = diff_table(_desired(comment="new"), _observed(comment="old"))

    assert isinstance(diff, TableDrift)
    assert any(
        isinstance(d, TableCommentDimension) and d.change == Changed(desired="new", observed="old")
        for d in diff.dimensions
    )


def test_partitioning_drift_produces_partitioning_dimension():
    diff = diff_table(
        _desired(partitioned_by=("id",)),
        _observed(),
    )

    assert isinstance(diff, TableDrift)
    assert any(isinstance(d, PartitioningDimension) for d in diff.dimensions)


def test_property_drift_produces_properties_dimension():
    diff = diff_table(
        _desired(properties={"a": "1", "b": "2"}),
        _observed(properties={"b": "9", "c": "3"}),
    )

    assert isinstance(diff, TableDrift)
    dim = next(d for d in diff.dimensions if isinstance(d, PropertiesDimension))
    assert set(dim.entries) == {
        Added(KeyValue("a", "1")),
        Changed(desired=KeyValue("b", "2"), observed=KeyValue("b", "9")),
        Removed(KeyValue("c", "3")),
    }


def test_tag_drift_produces_table_tags_dimension():
    diff = diff_table(
        _desired(tags={"env": "prod"}),
        _observed(tags={"stale": "yes"}),
    )

    assert isinstance(diff, TableDrift)
    dim = next(d for d in diff.dimensions if isinstance(d, TableTagsDimension))
    assert set(dim.entries) == {
        Added(KeyValue("env", "prod")),
        Removed(KeyValue("stale", "yes")),
    }


def test_primary_key_drift_produces_primary_key_dimension():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")
    diff = diff_table(
        _desired(columns=(Column("id", Integer(), nullable=False),), primary_key=pk),
        _observed(columns=(Column("id", Integer(), nullable=False),)),
    )

    assert isinstance(diff, TableDrift)
    assert any(isinstance(d, PrimaryKeyDimension) for d in diff.dimensions)


def test_foreign_key_drift_produces_foreign_keys_dimension():
    fk = _foreign_key()
    diff = diff_table(_desired(foreign_keys=(fk,)), _observed())

    assert isinstance(diff, TableDrift)
    assert any(isinstance(d, ForeignKeysDimension) for d in diff.dimensions)


def test_equal_primary_keys_by_column_set_produce_no_dimension():
    desired_pk = PrimaryKeyConstraint(columns=("a", "b"), constraint_name="test_pk")
    observed_pk = PrimaryKeyConstraint(columns=("b", "a"), constraint_name="other_name")
    columns = (Column("a", Integer(), nullable=False), Column("b", Integer(), nullable=False))

    diff = diff_table(
        _desired(columns=columns, primary_key=desired_pk),
        _observed(columns=columns, primary_key=observed_pk),
    )

    # Equal column sets → no PK dimension
    assert not any(isinstance(d, PrimaryKeyDimension) for d in diff.dimensions)


def test_equal_foreign_keys_by_signature_produce_no_dimension():
    diff = diff_table(
        _desired(foreign_keys=(_foreign_key("engine_name"),)),
        _observed(foreign_keys=(_foreign_key("external_name"),)),
    )

    assert not any(isinstance(d, ForeignKeysDimension) for d in diff.dimensions)


def test_changed_rejects_equal_values():
    # Given two equal values
    # Then Changed construction raises
    with pytest.raises(ValueError, match="no difference"):
        Changed(desired=42, observed=42)


def test_changed_accepts_unequal_values():
    # Given two different values
    result = Changed(desired=1, observed=2)

    # Then it holds both
    assert result.desired == 1
    assert result.observed == 2


def test_type_drift_produces_structure_dimension_with_type_entry_only():
    # Given a column where type, nullability, and comment all differ
    desired = _desired(columns=(Column("id", Integer(), nullable=False, comment="new"),))
    observed = _observed(columns=(Column("id", Long(), nullable=True, comment="old"),))

    # When diffing
    diff = diff_table(desired, observed)

    # Then the structure dimension contains only the type entry (nullability is
    # suppressed when type drifts — the column must be recreated first).
    # Comment drift is a separate dimension and is not suppressed.
    assert isinstance(diff, TableDrift)
    struct_dim = next(d for d in diff.dimensions if isinstance(d, ColumnStructureDimension))
    assert len(struct_dim.entries) == 1
    assert isinstance(struct_dim.entries[0], ColumnDataTypeChanged)
    comment_dim = next((d for d in diff.dimensions if isinstance(d, ColumnCommentsDimension)), None)
    assert comment_dim is not None


# ---------- ColumnStructureDimension


def test_column_structure_dimension_added_column_produces_add_column_only():
    # Given an added column with tags — tags come from ColumnTagsDimension, not here
    column = Column("age", Integer(), tags={"pii": "false"})
    dim = ColumnStructureDimension(entries=(ColumnAdded(column=column),))

    # Then only AddColumn is produced
    assert dim.actions() == (AddColumn(column=column),)


def test_column_structure_dimension_removed_column_produces_drop_column():
    column = Column("stale", Integer())
    dim = ColumnStructureDimension(entries=(ColumnRemoved(column=column),))

    assert dim.actions() == (DropColumn("stale"),)


def test_column_structure_dimension_nullability_produces_action():
    entry = ColumnNullabilityChanged(
        column_name="id", change=Changed(desired=True, observed=False)
    )
    dim = ColumnStructureDimension(entries=(entry,))

    assert dim.actions() == (SetColumnNullability(column_name="id", nullable=True),)


def test_column_structure_dimension_type_change_produces_no_action():
    entry = ColumnDataTypeChanged(
        column_name="id", change=Changed(desired=Integer(), observed=Long())
    )
    dim = ColumnStructureDimension(entries=(entry,))

    assert dim.actions() == ()


# ---------- ColumnCommentsDimension


def test_column_comments_dimension_diff_produces_entries_for_matched_columns_only():
    # Given a desired table with a matched column (comment differs) and a desired-only column
    desired = (Column("id", Integer(), comment="pk"), Column("ghost", String(), comment="x"))
    observed = (Column("id", Integer(), comment=""),)

    dim = ColumnCommentsDimension.diff(desired, observed)

    # Then only the matched column produces an entry; the ghost column is not diffed
    assert dim is not None
    assert len(dim.entries) == 1
    assert dim.entries[0].column_name == "id"


def test_column_comments_dimension_produces_set_column_comment():
    entry = ColumnCommentChanged(column_name="id", change=Changed(desired="pk", observed=""))
    dim = ColumnCommentsDimension(entries=(entry,))

    assert dim.actions() == (SetColumnComment("id", "pk"),)


def test_column_comments_dimension_returns_none_when_no_comment_drift():
    desired = (Column("id", Integer(), comment="same"),)
    observed = (Column("id", Integer(), comment="same"),)

    assert ColumnCommentsDimension.diff(desired, observed) is None


# ---------- ColumnTagsDimension


def test_column_tags_dimension_covers_added_columns():
    # Given a desired-only column with tags (it will be created by ColumnStructureDimension)
    desired = (Column("id", Integer()), Column("new", String(), tags={"pii": "true"}))
    observed = (Column("id", Integer()),)

    dim = ColumnTagsDimension.diff(desired, observed)

    # Then the added column's tags are included (ADD_COLUMN precedes SET_COLUMN_TAG)
    assert dim is not None
    assert any(e.column_name == "new" for e in dim.entries)


def test_column_tags_dimension_tag_entry_produces_set_and_unset():
    entry = ColumnTagsChanged(
        column_name="id",
        entries=(
            Added(KeyValue("new", "x")),
            Changed(desired=KeyValue("pii", "true"), observed=KeyValue("pii", "false")),
            Removed(KeyValue("old", "y")),
        ),
    )
    dim = ColumnTagsDimension(entries=(entry,))

    assert set(dim.actions()) == {
        SetColumnTag(column_name="id", name="new", value="x"),
        SetColumnTag(column_name="id", name="pii", value="true"),
        UnsetColumnTag(column_name="id", name="old"),
    }


def test_column_tags_dimension_returns_none_when_no_tag_drift():
    desired = (Column("id", Integer(), tags={"pii": "true"}),)
    observed = (Column("id", Integer(), tags={"pii": "true"}),)

    assert ColumnTagsDimension.diff(desired, observed) is None


def test_table_comment_dimension_produces_set_table_comment():
    # Given a changed table comment
    dim = TableCommentDimension(change=Changed(desired="new", observed="old"))

    # Then a single SetTableComment action is produced
    assert dim.actions() == (SetTableComment(comment="new"),)


def test_properties_dimension_sets_added_and_changed_ignores_removed():
    # Given properties drifting in all three ways
    dim = PropertiesDimension(
        entries=(
            Added(KeyValue("a", "1")),
            Changed(desired=KeyValue("b", "2"), observed=KeyValue("b", "9")),
            Removed(KeyValue("c", "3")),
        )
    )

    # Then only Added and Changed entries produce SetProperty; Removed is silently ignored
    assert set(dim.actions()) == {
        SetProperty(name="a", value="1"),
        SetProperty(name="b", value="2"),
    }


def test_table_tags_dimension_sets_and_unsets_with_full_state_semantics():
    # Given tags drifting with one addition and one removal
    dim = TableTagsDimension(
        entries=(
            Added(KeyValue("env", "prod")),
            Removed(KeyValue("stale", "yes")),
        )
    )

    # When actions are requested
    # Then Added produces SetTableTag and Removed produces UnsetTableTag
    assert set(dim.actions()) == {
        SetTableTag(name="env", value="prod"),
        UnsetTableTag(name="stale"),
    }


def test_partitioning_dimension_produces_no_actions():
    # Given a partitioning change — no in-place action is possible
    dim = PartitioningDimension(change=Changed(desired=("ds",), observed=()))

    # Then no action is produced; PartitioningChangeNotSupported raises the failure
    assert dim.actions() == ()


def test_primary_key_dimension_added_produces_set_primary_key():
    # Given an added primary key
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")
    dim = PrimaryKeyDimension(entry=Added(pk))

    # Then SetPrimaryKey is produced
    assert dim.actions() == (SetPrimaryKey(columns=("id",), constraint_name="test_pk"),)


def test_primary_key_dimension_removed_produces_drop_primary_key():
    # Given a removed primary key
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="legacy_pk")
    dim = PrimaryKeyDimension(entry=Removed(pk))

    # Then DropPrimaryKey is produced
    assert dim.actions() == (DropPrimaryKey(),)


def test_primary_key_dimension_changed_produces_drop_then_set():
    # Given a changed primary key (column set differs)
    desired_pk = PrimaryKeyConstraint(columns=("a",), constraint_name="test_pk")
    observed_pk = PrimaryKeyConstraint(columns=("b",), constraint_name="test_pk")
    dim = PrimaryKeyDimension(entry=Changed(desired=desired_pk, observed=observed_pk))

    # When the actions are sorted by ActionPlan (drop runs before set)
    plan = ActionPlan(dim.actions())

    # Then the plan contains DropPrimaryKey followed by SetPrimaryKey
    assert plan.actions == (
        DropPrimaryKey(),
        SetPrimaryKey(columns=("a",), constraint_name="test_pk"),
    )


def _fk(constraint_name: str = "test_fk") -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "other"),
        referenced_columns=("id",),
        constraint_name=constraint_name,
    )


def test_foreign_keys_dimension_added_produces_set_foreign_key():
    # Given an added foreign key
    dim = ForeignKeysDimension(entries=(Added(_fk()),))

    # Then SetForeignKey is produced with all FK fields
    assert dim.actions() == (
        SetForeignKey(
            local_columns=("id",),
            referenced_table=QualifiedName("dev", "silver", "other"),
            referenced_columns=("id",),
            constraint_name="test_fk",
        ),
    )


def test_foreign_keys_dimension_removed_produces_drop_foreign_key():
    # Given a removed foreign key
    dim = ForeignKeysDimension(entries=(Removed(_fk("stale_fk")),))

    # Then DropForeignKey is produced with the constraint name
    assert dim.actions() == (DropForeignKey(constraint_name="stale_fk"),)
