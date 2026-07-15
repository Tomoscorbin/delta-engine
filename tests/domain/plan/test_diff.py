import pytest

from delta_engine.domain.model import (
    ALL_ASPECTS,
    DesiredColumn,
    DesiredTable,
    ForeignKeyReference,
    Integer,
    Long,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
    TableAspect,
)
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
from delta_engine.domain.plan.actions import (
    AddColumn,
    AlterClustering,
    AlterColumnType,
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
from delta_engine.domain.plan.diff import (
    ColumnRenameConflict,
    PartitioningChanged,
    PropertyUndeclared,
    TableDrift,
    TableMissing,
    diff_table,
)
from tests.builders import as_observed_columns

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _desired(**overrides) -> DesiredTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(DesiredColumn("id", Integer()),))
    return DesiredTable(**{**defaults, **overrides})


def _observed(**overrides) -> ObservedTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(ObservedColumn("id", Integer()),))
    merged = {**defaults, **overrides}
    merged["columns"] = as_observed_columns(merged["columns"])
    return ObservedTable(**merged)


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
    assert diff.actions == ()
    assert diff.findings == ()


def test_drift_carries_the_desired_table():
    # Given a desired table
    desired = _desired()

    # When diffing
    diff = diff_table(desired, _observed())

    # Then the drift is self-contained: it carries the declaration, so
    # validation reads scope and properties from it with no second argument
    assert isinstance(diff, TableDrift)
    assert diff.desired is desired
    assert not hasattr(diff, "plan")


# ---------- column structure changes


def test_desired_only_column_produces_column_added_change():
    # Given a desired table with an extra column not in the observed table
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer()), DesiredColumn("age", Integer()))),
        _observed(),
    )

    # Then a AddColumn change is produced
    assert isinstance(diff, TableDrift)
    assert AddColumn(DesiredColumn("age", Integer())) in diff.actions


def test_observed_only_column_produces_column_removed_change():
    # Given an observed table with an extra column not in the desired table
    diff = diff_table(
        _desired(),
        _observed(columns=(DesiredColumn("id", Integer()), DesiredColumn("stale", String()))),
    )

    # Then a DropColumn change is produced
    assert isinstance(diff, TableDrift)
    assert diff.actions == (DropColumn(ObservedColumn("stale", String())),)


def test_type_drift_produces_column_data_type_changed():
    # Given a column whose data type differs between desired and observed
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer()),)),
        _observed(columns=(DesiredColumn("id", Long()),)),
    )

    # Then a AlterColumnType change carries both sides
    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        AlterColumnType(column_name="id", desired_type=Integer(), observed_type=Long()),
    )


def test_type_drift_suppresses_nullability_change_but_not_comment_change():
    # Given a column where type, nullability, and comment all differ
    desired = _desired(columns=(DesiredColumn("id", Integer(), nullable=False, comment="new"),))
    observed = _observed(columns=(DesiredColumn("id", Long(), nullable=True, comment="old"),))

    # When diffing
    diff = diff_table(desired, observed)

    # Then the type change is present and nullability is suppressed (the column
    # must be recreated first); comment drift is independent and not suppressed
    assert isinstance(diff, TableDrift)
    assert any(isinstance(change, AlterColumnType) for change in diff.actions)
    assert not any(isinstance(change, SetColumnNullability) for change in diff.actions)
    assert any(isinstance(change, SetColumnComment) for change in diff.actions)


def test_nullability_drift_produces_column_nullability_changed():
    # Given a column whose nullability differs
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer(), nullable=False),)),
        _observed(columns=(DesiredColumn("id", Integer(), nullable=True),)),
    )

    # Then a SetColumnNullability change carries the direction
    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        SetColumnNullability(column_name="id", desired_nullable=False, observed_nullable=True),
    )


# ---------- column comment changes


def test_comment_drift_on_matched_column_produces_change():
    # Given a matched column with differing comments and a desired-only column
    diff = diff_table(
        _desired(
            columns=(
                DesiredColumn("id", Integer(), comment="pk"),
                DesiredColumn("ghost", String(), comment="x"),
            )
        ),
        _observed(columns=(DesiredColumn("id", Integer(), comment=""),)),
    )

    # Then only the matched column produces a comment change; the ghost column's
    # comment travels inside its AddColumn change
    assert isinstance(diff, TableDrift)
    comment_changes = [change for change in diff.actions if isinstance(change, SetColumnComment)]
    assert comment_changes == [
        SetColumnComment(column_name="id", desired_comment="pk", observed_comment="")
    ]


# ---------- column tag changes


def test_column_tag_drift_produces_set_and_unset_changes():
    # Given a column with one tag to set, one to update, and one to remove
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer(), tags={"new": "x", "pii": "true"}),)),
        _observed(columns=(DesiredColumn("id", Integer(), tags={"pii": "false", "old": "y"}),)),
    )

    # Then set changes cover added and updated tags; an unset change covers the removed tag
    assert isinstance(diff, TableDrift)
    tag_changes = {
        change for change in diff.actions if isinstance(change, (SetColumnTag, UnsetColumnTag))
    }
    assert tag_changes == {
        SetColumnTag(column_name="id", name="new", value="x"),
        SetColumnTag(column_name="id", name="pii", value="true"),
        UnsetColumnTag(column_name="id", name="old"),
    }


def test_added_columns_tags_produce_set_facts():
    # Given a desired-only column with tags (created by its AddColumn change)
    diff = diff_table(
        _desired(
            columns=(
                DesiredColumn("id", Integer()),
                DesiredColumn("new", String(), tags={"pii": "true"}),
            )
        ),
        _observed(),
    )

    # Then the added column's tags are changes too — ADD_COLUMN precedes SET_COLUMN_TAG
    assert isinstance(diff, TableDrift)
    assert SetColumnTag(column_name="new", name="pii", value="true") in diff.actions


def test_identical_column_tags_produce_no_changes():
    # Given matched columns with identical tags
    columns = (DesiredColumn("id", Integer(), tags={"pii": "true"}),)
    diff = diff_table(_desired(columns=columns), _observed(columns=columns))

    # Then no tag changes are produced
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.findings == ()


# ---------- table comment change


def test_table_comment_drift_produces_change_with_both_sides():
    diff = diff_table(_desired(comment="new"), _observed(comment="old"))

    assert isinstance(diff, TableDrift)
    assert diff.actions == (SetTableComment(desired_comment="new", observed_comment="old"),)


# ---------- property changes (exact declaration)


def test_declared_property_absent_from_catalog_produces_first_write_set():
    # Given a declared key the catalog lacks
    diff = diff_table(
        _desired(properties={"delta.enableChangeDataFeed": "true"}),
        _observed(properties={}),
    )

    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        SetProperty(name="delta.enableChangeDataFeed", desired_value="true", observed_value=None),
    )


def test_declared_property_with_stale_value_produces_set_carrying_both_sides():
    diff = diff_table(
        _desired(properties={"delta.enableChangeDataFeed": "true"}),
        _observed(properties={"delta.enableChangeDataFeed": "false"}),
    )

    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        SetProperty(
            name="delta.enableChangeDataFeed", desired_value="true", observed_value="false"
        ),
    )


def test_declared_property_matching_catalog_produces_no_change():
    diff = diff_table(
        _desired(properties={"delta.enableChangeDataFeed": "true"}),
        _observed(properties={"delta.enableChangeDataFeed": "true"}),
    )

    # Then no change is produced — the property sync is idempotent
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.findings == ()


def test_none_declaration_on_present_key_produces_unset():
    # Given a declaration asserting the key must be absent, and a catalog that has it
    diff = diff_table(
        _desired(properties={"delta.logRetentionDuration": None}),
        _observed(properties={"delta.logRetentionDuration": "interval 30 days"}),
    )

    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        UnsetProperty(name="delta.logRetentionDuration", observed_value="interval 30 days"),
    )


def test_none_declaration_on_absent_key_produces_no_change():
    # Given an absence assertion that already holds
    diff = diff_table(
        _desired(properties={"delta.logRetentionDuration": None}),
        _observed(properties={}),
    )

    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.findings == ()


def test_undeclared_registered_key_produces_an_undeclared_property_finding():
    # Given a registered key on the table that the declaration omits
    diff = diff_table(
        _desired(properties={}),
        _observed(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then the diff records the missing declaration intent as a finding
    assert isinstance(diff, TableDrift)
    assert diff.findings == (
        PropertyUndeclared(name="delta.columnMapping.mode", observed_value="name"),
    )


def test_every_observed_key_without_declaration_produces_a_finding():
    # Given an observed managed key the declaration omits — the reader filters
    # the catalog map to managed keys, so every surviving key demands a decision
    diff = diff_table(
        _desired(properties={}),
        _observed(properties={"delta.enableChangeDataFeed": "true"}),
    )

    assert isinstance(diff, TableDrift)
    assert diff.findings == (
        PropertyUndeclared(name="delta.enableChangeDataFeed", observed_value="true"),
    )


def test_properties_diff_is_skipped_when_properties_unmanaged():
    # Given a declaration that does not manage properties (metadata-only style)
    # over a catalog carrying an undeclared registered key
    managed = ALL_ASPECTS - frozenset({TableAspect.PROPERTIES})
    diff = diff_table(
        _desired(properties={}, managed_aspects=managed),
        _observed(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then no property change of any kind — no assertion was made
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.findings == ()


def test_declared_properties_are_not_compared_when_properties_unmanaged():
    # Given a declaration that carries a property but does not manage
    # properties, over a catalog where that property is absent
    managed = ALL_ASPECTS - frozenset({TableAspect.PROPERTIES})
    diff = diff_table(
        _desired(properties={"delta.enableChangeDataFeed": "true"}, managed_aspects=managed),
        _observed(properties={}),
    )

    # Then the carried property makes no assertion and produces no change
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.findings == ()


def test_property_set_rejects_equal_values():
    with pytest.raises(ValueError, match="no difference"):
        SetProperty(name="delta.enableChangeDataFeed", desired_value="true", observed_value="true")


def test_property_set_first_write_is_always_representable():
    # Given observed_value=None — the guard is bypassed by type
    change = SetProperty(
        name="delta.enableChangeDataFeed", desired_value="true", observed_value=None
    )

    assert change.observed_value is None


# ---------- table tag changes (full-state)


def test_table_tag_drift_produces_set_and_unset_changes():
    # Given one declared tag missing from the catalog and one observed-only tag
    diff = diff_table(
        _desired(tags={"env": "prod"}),
        _observed(tags={"stale": "yes"}),
    )

    # Then the declared tag is set and the undeclared tag is unset — full-state
    assert isinstance(diff, TableDrift)
    assert set(diff.actions) == {
        SetTableTag(name="env", value="prod"),
        UnsetTableTag(name="stale"),
    }


# ---------- partitioning change


def test_partitioning_drift_produces_change_with_both_sides():
    # Given identical columns on both sides so partitioning is the only drift
    columns = (DesiredColumn("id", Integer()), DesiredColumn("value", Integer()))
    diff = diff_table(
        _desired(columns=columns, partitioned_by=("id",)),
        _observed(columns=columns),
    )

    assert isinstance(diff, TableDrift)
    assert diff.findings == (
        PartitioningChanged(desired_partitioning=("id",), observed_partitioning=()),
    )


# ---------- clustering change


def test_clustering_drift_produces_change_with_both_sides():
    # Given identical columns so clustering is the only drift
    columns = (DesiredColumn("id", Integer()), DesiredColumn("region", String()))
    diff = diff_table(
        _desired(columns=columns, clustered_by=("region",)),
        _observed(columns=columns, clustered_by=()),
    )
    assert diff.actions == (
        AlterClustering(desired_clustering=("region",), observed_clustering=()),
    )


def test_reordered_clustering_keys_are_not_a_change():
    # Given the same clustering key set in a different order on each side
    columns = (DesiredColumn("id", Integer()), DesiredColumn("region", String()))
    diff = diff_table(
        _desired(columns=columns, clustered_by=("region", "id")),
        _observed(columns=columns, clustered_by=("id", "region")),
    )
    # Then no clustering change is produced — key order is immaterial
    assert diff.actions == ()
    assert diff.findings == ()


def test_clustering_removal_produces_cluster_by_none_action():
    # Given a table clustered in the catalog but declared unclustered
    columns = (DesiredColumn("id", Integer()), DesiredColumn("region", String()))
    diff = diff_table(
        _desired(columns=columns, clustered_by=()),
        _observed(columns=columns, clustered_by=("region",)),
    )
    assert diff.actions == (
        AlterClustering(desired_clustering=(), observed_clustering=("region",)),
    )


def test_clustering_changed_rejects_equal_key_sets():
    with pytest.raises(ValueError, match="no difference"):
        AlterClustering(desired_clustering=("a", "b"), observed_clustering=("b", "a"))


# ---------- primary key changes


def test_desired_only_primary_key_produces_added_change():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer(), nullable=False),), primary_key=pk),
        _observed(columns=(DesiredColumn("id", Integer(), nullable=False),)),
    )

    assert isinstance(diff, TableDrift)
    assert diff.actions == (SetPrimaryKey(primary_key=pk),)


def test_equal_primary_keys_by_column_set_produce_no_change():
    # Given the same PK column set under different orders and names
    desired_pk = PrimaryKeyConstraint(columns=("a", "b"), constraint_name="test_pk")
    observed_pk = PrimaryKeyConstraint(columns=("b", "a"), constraint_name="other_name")
    columns = (
        DesiredColumn("a", Integer(), nullable=False),
        DesiredColumn("b", Integer(), nullable=False),
    )

    diff = diff_table(
        _desired(columns=columns, primary_key=desired_pk),
        _observed(columns=columns, primary_key=observed_pk),
    )

    # Then identity is column-set equality — no change
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.findings == ()


# ---------- foreign key changes


def test_desired_only_foreign_key_produces_added_change():
    fk = _foreign_key()
    diff = diff_table(_desired(foreign_keys=(fk,)), _observed())

    assert isinstance(diff, TableDrift)
    assert diff.actions == (SetForeignKey(constraint=fk),)


def test_equal_foreign_keys_by_signature_produce_no_change():
    # Given the same FK relationship under different constraint names
    diff = diff_table(
        _desired(foreign_keys=(_foreign_key("engine_name"),)),
        _observed(foreign_keys=(_foreign_key("external_name"),)),
    )

    # Then identity is the content signature — no change, sync stays idempotent
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.findings == ()


# ---------- no-difference changes are unrepresentable


def test_column_data_type_changed_rejects_equal_types():
    with pytest.raises(ValueError, match="no difference"):
        AlterColumnType(column_name="id", desired_type=Integer(), observed_type=Integer())


def test_column_nullability_changed_rejects_equal_flags():
    with pytest.raises(ValueError, match="no difference"):
        SetColumnNullability(column_name="id", desired_nullable=True, observed_nullable=True)


def test_column_comment_changed_rejects_equal_comments():
    with pytest.raises(ValueError, match="no difference"):
        SetColumnComment(column_name="id", desired_comment="same", observed_comment="same")


def test_table_comment_changed_rejects_equal_comments():
    with pytest.raises(ValueError, match="no difference"):
        SetTableComment(desired_comment="same", observed_comment="same")


def test_partitioning_changed_rejects_equal_specs():
    with pytest.raises(ValueError, match="no difference"):
        PartitioningChanged(desired_partitioning=("ds",), observed_partitioning=("ds",))


def test_observed_only_primary_key_produces_removed_change():
    # Given a catalog primary key that is absent from the declaration
    primary_key = PrimaryKeyConstraint(columns=("id",), constraint_name="legacy_pk")

    # When diffing
    diff = diff_table(
        _desired(),
        _observed(primary_key=primary_key),
    )

    # Then the primary key is marked for removal
    assert isinstance(diff, TableDrift)
    assert diff.actions == (DropPrimaryKey(primary_key=primary_key, referencing_foreign_keys=()),)


def test_primary_key_removal_carries_observed_referencing_foreign_keys():
    # Given a catalog primary key that other tables reference by foreign key
    reference = ForeignKeyReference(
        constraint_name="orders_customer_id_fk",
        referencing_table=QualifiedName("dev", "silver", "orders"),
    )
    primary_key = PrimaryKeyConstraint(columns=("id",), constraint_name="customers_pk")

    # When diffing a declaration that drops the primary key
    diff = diff_table(
        _desired(),
        _observed(primary_key=primary_key, referencing_foreign_keys=(reference,)),
    )

    # Then the removal change carries the inbound reference for validation to judge
    assert isinstance(diff, TableDrift)
    (change,) = diff.actions
    assert isinstance(change, DropPrimaryKey)
    assert change.referencing_foreign_keys == (reference,)


def test_changed_primary_key_produces_drop_and_set_actions():
    # Given desired and observed primary keys over different column sets
    desired_primary_key = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")
    observed_primary_key = PrimaryKeyConstraint(columns=("other_id",), constraint_name="legacy_pk")
    columns = (
        DesiredColumn("id", Integer(), nullable=False),
        DesiredColumn("other_id", Integer(), nullable=False),
    )

    # When diffing
    diff = diff_table(
        _desired(columns=columns, primary_key=desired_primary_key),
        _observed(columns=columns, primary_key=observed_primary_key),
    )

    # Then the observed key is dropped and the desired key is set
    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        DropPrimaryKey(
            primary_key=observed_primary_key,
            referencing_foreign_keys=(),
        ),
        SetPrimaryKey(primary_key=desired_primary_key),
    )


def test_primary_key_change_carries_observed_referencing_foreign_keys():
    # Given desired and observed primary keys over different column sets, where
    # another table references the observed key by foreign key
    reference = ForeignKeyReference(
        constraint_name="orders_customer_id_fk",
        referencing_table=QualifiedName("dev", "silver", "orders"),
    )
    desired_primary_key = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")
    observed_primary_key = PrimaryKeyConstraint(columns=("other_id",), constraint_name="legacy_pk")
    columns = (
        DesiredColumn("id", Integer(), nullable=False),
        DesiredColumn("other_id", Integer(), nullable=False),
    )

    # When diffing a declaration that changes the primary key
    diff = diff_table(
        _desired(columns=columns, primary_key=desired_primary_key),
        _observed(
            columns=columns,
            primary_key=observed_primary_key,
            referencing_foreign_keys=(reference,),
        ),
    )

    # Then the drop half carries the inbound reference for validation to judge
    assert isinstance(diff, TableDrift)
    drop = next(change for change in diff.actions if isinstance(change, DropPrimaryKey))
    assert drop.referencing_foreign_keys == (reference,)


def test_observed_only_foreign_key_produces_removed_change():
    # Given a catalog foreign key that is absent from the declaration
    foreign_key = _foreign_key("legacy_fk")

    # When diffing
    diff = diff_table(
        _desired(),
        _observed(foreign_keys=(foreign_key,)),
    )

    # Then the foreign key is marked for removal
    assert isinstance(diff, TableDrift)
    assert diff.actions == (DropForeignKey(constraint=foreign_key),)


def test_changed_foreign_key_signature_produces_remove_and_add_changes():
    # Given a declared FK and an observed FK with different relationship signatures
    desired_foreign_key = _foreign_key("desired_fk")
    observed_foreign_key = ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "different_parent"),
        referenced_columns=("id",),
        constraint_name="legacy_fk",
    )

    # When diffing
    diff = diff_table(
        _desired(foreign_keys=(desired_foreign_key,)),
        _observed(foreign_keys=(observed_foreign_key,)),
    )

    # Then the observed relationship is removed and the desired relationship is added
    assert isinstance(diff, TableDrift)
    assert set(diff.actions) == {
        SetForeignKey(constraint=desired_foreign_key),
        DropForeignKey(constraint=observed_foreign_key),
    }


def test_observed_only_column_tags_are_ignored_because_column_is_removed():
    # Given an observed-only column that also has catalog tags
    observed_only_column = ObservedColumn("stale", Integer(), tags={"old": "true"})

    # When diffing
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer()),)),
        _observed(columns=(DesiredColumn("id", Integer()), observed_only_column)),
    )

    # Then the column removal is enough; no tag-unset noise is produced for a
    # column that will be dropped
    assert isinstance(diff, TableDrift)
    assert diff.actions == (DropColumn(observed_only_column),)


# ---------- column renames


def test_diff_emits_rename_when_source_observed_and_target_absent():
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("customer_name", String(), renamed_from="customer_nm"),
        )
    )
    observed = _observed(
        columns=(DesiredColumn("id", Integer()), DesiredColumn("customer_nm", String()))
    )

    drift = diff_table(desired, observed)

    assert drift.actions == (RenameColumn(old_name="customer_nm", new_name="customer_name"),)


def test_diff_pairs_residual_drift_under_the_new_name():
    # A rename plus a widen decomposes into RenameColumn + AlterColumnType
    desired = _desired(columns=(DesiredColumn("amount", Long(), renamed_from="amt"),))
    observed = _observed(columns=(DesiredColumn("amt", Integer()),))

    drift = diff_table(desired, observed)

    assert RenameColumn(old_name="amt", new_name="amount") in drift.actions
    assert (
        AlterColumnType(column_name="amount", desired_type=Long(), observed_type=Integer())
        in drift.actions
    )
    assert not any(isinstance(c, AddColumn | DropColumn) for c in drift.actions)


def test_diff_hint_is_inert_when_applied_rename_is_steady_state():
    desired = _desired(
        columns=(DesiredColumn("customer_name", String(), renamed_from="customer_nm"),)
    )
    applied = _observed(columns=(DesiredColumn("customer_name", String()),))
    drift = diff_table(desired, applied)
    assert drift.actions == ()
    assert drift.findings == ()


def test_diff_hint_is_a_plain_add_when_neither_name_is_observed():
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("customer_name", String(), renamed_from="customer_nm"),
        )
    )
    observed = _observed(columns=(DesiredColumn("id", Integer()),))

    actions = diff_table(desired, observed).actions

    assert actions == (
        AddColumn(column=DesiredColumn("customer_name", String(), renamed_from="customer_nm")),
    )


def test_diff_emits_explicit_conflict_when_source_and_target_both_observed():
    desired = _desired(
        columns=(DesiredColumn("customer_name", String(), renamed_from="customer_nm"),)
    )
    observed = _observed(
        columns=(DesiredColumn("customer_name", String()), DesiredColumn("customer_nm", String()))
    )

    drift = diff_table(desired, observed)

    # The conflict is a finding, and the source column is neither dropped nor renamed
    assert drift.findings == (
        ColumnRenameConflict(old_name="customer_nm", new_name="customer_name"),
    )
    assert drift.actions == ()


def test_diff_missing_table_ignores_rename_hints():
    desired = _desired(
        columns=(DesiredColumn("customer_name", String(), renamed_from="customer_nm"),)
    )

    diff = diff_table(desired, None)

    assert isinstance(diff, TableMissing)
    assert diff.desired is desired
    assert not hasattr(diff, "plan")


def test_diff_projects_clustering_identity_across_a_rename():
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("customer_name", String(), renamed_from="customer_nm"),
        ),
        clustered_by=("customer_name",),
    )
    observed = _observed(
        columns=(DesiredColumn("id", Integer()), DesiredColumn("customer_nm", String())),
        clustered_by=("customer_nm",),
    )

    actions = diff_table(desired, observed).actions

    assert actions == (RenameColumn(old_name="customer_nm", new_name="customer_name"),)


def test_diff_projects_partition_identity_across_a_rename():
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("event_day", String(), renamed_from="day"),
        ),
        partitioned_by=("event_day",),
    )
    observed = _observed(
        columns=(DesiredColumn("id", Integer()), DesiredColumn("day", String())),
        partitioned_by=("day",),
    )

    actions = diff_table(desired, observed).actions

    assert actions == (RenameColumn(old_name="day", new_name="event_day"),)


def test_diff_rename_and_primary_key_replacement_are_direct_actions():
    desired_key = PrimaryKeyConstraint(columns=("customer_name",), constraint_name="test_pk")
    observed_key = PrimaryKeyConstraint(columns=("customer_nm",), constraint_name="legacy_pk")
    desired = _desired(
        columns=(
            DesiredColumn("customer_name", String(), nullable=False, renamed_from="customer_nm"),
        ),
        primary_key=desired_key,
    )
    observed = _observed(
        columns=(DesiredColumn("customer_nm", String(), nullable=False),),
        primary_key=observed_key,
    )

    drift = diff_table(desired, observed)

    assert set(drift.actions) == {
        RenameColumn(old_name="customer_nm", new_name="customer_name"),
        DropPrimaryKey(
            primary_key=observed_key,
            referencing_foreign_keys=(),
        ),
        SetPrimaryKey(primary_key=desired_key),
    }


def test_diff_rename_and_foreign_key_replacement_are_direct_actions():
    parent = QualifiedName("dev", "silver", "parent")
    desired_key = ForeignKeyConstraint(
        local_columns=("parent_id",),
        referenced_table=parent,
        referenced_columns=("id",),
        constraint_name="test_parent_id_fk",
    )
    observed_key = ForeignKeyConstraint(
        local_columns=("parent",),
        referenced_table=parent,
        referenced_columns=("id",),
        constraint_name="legacy_fk",
    )
    desired = _desired(
        columns=(DesiredColumn("parent_id", Integer(), renamed_from="parent"),),
        foreign_keys=(desired_key,),
    )
    observed = _observed(
        columns=(DesiredColumn("parent", Integer()),),
        foreign_keys=(observed_key,),
    )

    drift = diff_table(desired, observed)

    assert set(drift.actions) == {
        RenameColumn(old_name="parent", new_name="parent_id"),
        SetForeignKey(constraint=desired_key),
        DropForeignKey(constraint=observed_key),
    }


def test_diff_rename_and_self_referenced_foreign_key_replacement_are_direct_actions():
    desired_key = ForeignKeyConstraint(
        local_columns=("manager_id",),
        referenced_table=_QUALIFIED_NAME,
        referenced_columns=("employee_id",),
        constraint_name="test_manager_id_fk",
    )
    observed_key = ForeignKeyConstraint(
        local_columns=("manager_id",),
        referenced_table=_QUALIFIED_NAME,
        referenced_columns=("id",),
        constraint_name="legacy_fk",
    )
    desired = _desired(
        columns=(
            DesiredColumn("employee_id", Integer(), renamed_from="id"),
            DesiredColumn("manager_id", Integer()),
        ),
        foreign_keys=(desired_key,),
    )
    observed = _observed(
        columns=(DesiredColumn("id", Integer()), DesiredColumn("manager_id", Integer())),
        foreign_keys=(observed_key,),
    )

    drift = diff_table(desired, observed)

    assert set(drift.actions) == {
        RenameColumn(old_name="id", new_name="employee_id"),
        SetForeignKey(constraint=desired_key),
        DropForeignKey(constraint=observed_key),
    }


def test_diff_keeps_an_unrelated_foreign_key_drop_alongside_a_rename():
    unrelated_key = ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "parent"),
        referenced_columns=("id",),
        constraint_name="legacy_fk",
    )
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("customer_name", String(), renamed_from="customer_nm"),
        )
    )
    observed = _observed(
        columns=(DesiredColumn("id", Integer()), DesiredColumn("customer_nm", String())),
        foreign_keys=(unrelated_key,),
    )

    drift = diff_table(desired, observed)

    assert drift.actions == (
        RenameColumn(old_name="customer_nm", new_name="customer_name"),
        DropForeignKey(constraint=unrelated_key),
    )
