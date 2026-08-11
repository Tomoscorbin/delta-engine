import pytest

from delta_engine.domain.model import (
    Array,
    DesiredColumn,
    DesiredTable,
    Identifier,
    Integer,
    Long,
    Map,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
    Struct,
    StructField,
    TableFeature,
    TableKind,
    TableScope,
    TimestampNtz,
    Variant,
)
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
from delta_engine.domain.plan.actions import (
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
from delta_engine.domain.plan.diff import (
    TableCreation,
    TableDrift,
    diff_table,
)
from delta_engine.domain.plan.unresolvable import (
    ColumnCaseDrift,
    ColumnRenameConflict,
    PartitioningChanged,
    PropertyUndeclared,
)
from tests.builders import as_observed_columns

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")
_PARENT_NAME = QualifiedName("dev", "silver", "other")


def _desired(**overrides) -> DesiredTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(DesiredColumn("id", Integer()),))
    return DesiredTable(**{**defaults, **overrides})


def _observed(**overrides) -> ObservedTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(ObservedColumn("id", Integer()),))
    merged = {**defaults, **overrides}
    merged["columns"] = as_observed_columns(merged["columns"])
    return ObservedTable(**merged)


def _foreign_key(
    constraint_name: str | None = "test_id_fk",
    local_columns: tuple[str, ...] = ("id",),
    referenced_columns: tuple[str, ...] = ("id",),
    referenced_table: QualifiedName = _PARENT_NAME,
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=local_columns,
        referenced_table=referenced_table,
        referenced_columns=referenced_columns,
        constraint_name=constraint_name,
    )


# ---------- top-level diff sum


def test_missing_table_diffs_to_table_missing_carrying_desired():
    # Given no observed table
    desired = _desired()

    # When diffing against None
    diff = diff_table(desired, observed=None)

    # Then the diff is the self-contained missing-table variant
    assert diff == TableCreation(desired=desired)


def test_missing_table_derives_target_from_desired_table():
    desired = _desired()

    diff = diff_table(desired, observed=None)

    assert isinstance(diff, TableCreation)
    assert diff.target == _QUALIFIED_NAME


def test_missing_table_actions_are_exactly_the_table_creation():
    # Given a table with no observed counterpart
    desired = _desired()

    diff = diff_table(desired, observed=None)

    # Then the derived actions are exactly the create
    assert diff.actions == (CreateTable(desired),)


def test_equal_tables_diff_to_empty_drift():
    # Given identical desired and observed definitions
    diff = diff_table(_desired(), _observed())

    # Then no changes are produced — the natural zero
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.unresolvable == ()


def test_drift_carries_the_desired_table():
    # Given a desired table
    desired = _desired()

    # When diffing
    diff = diff_table(desired, _observed())

    # Then the drift is self-contained: it carries the declaration, so
    # validation reads scope and properties from it with no second argument
    assert isinstance(diff, TableDrift)
    assert diff.desired is desired


def test_drift_derives_target_from_the_shared_table_identity():
    # Given matching desired and observed table identities
    diff = diff_table(_desired(), _observed())

    # Then the comparison exposes their one shared target
    assert isinstance(diff, TableDrift)
    assert diff.target == _QUALIFIED_NAME


def test_drift_rejects_different_desired_and_observed_tables():
    # Given a desired and observed referencing different tables
    desired = _desired(qualified_name=QualifiedName("cat", "sch", "customers"))
    observed = _observed(qualified_name=QualifiedName("cat", "sch", "orders"))

    # When / Then constructing their comparison is rejected
    with pytest.raises(ValueError):
        TableDrift(desired=desired, observed=observed)


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


@pytest.mark.parametrize(
    ("desired_type", "observed_type"),
    [
        (Integer(), Long()),
        (
            Struct((StructField("value", Integer(), nullable=False),)),
            Struct((StructField("value", Integer(), nullable=True),)),
        ),
    ],
    ids=["scalar-type", "struct-field-nullability"],
)
def test_type_drift_produces_column_data_type_changed(desired_type, observed_type):
    # Given a column whose modeled data type differs between desired and observed
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", desired_type),)),
        _observed(columns=(DesiredColumn("id", observed_type),)),
    )

    # Then an AlterColumnType change carries both complete modeled types
    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        AlterColumnType(
            column_name="id",
            desired_type=desired_type,
            observed_type=observed_type,
        ),
    )


def test_type_nullability_and_comment_drift_on_one_column_each_produce_a_change():
    # Given one column whose type, nullability, and comment all differ
    desired = _desired(columns=(DesiredColumn("id", Integer(), nullable=False, comment="new"),))
    observed = _observed(columns=(DesiredColumn("id", Long(), nullable=True, comment="old"),))

    # When diffing
    diff = diff_table(desired, observed)

    # Then each aspect drifts independently and produces its own change
    assert isinstance(diff, TableDrift)
    assert any(isinstance(change, AlterColumnType) for change in diff.actions)
    assert any(isinstance(change, SetColumnNullability) for change in diff.actions)
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


# ---------- schema-implied table features


def test_existing_table_diff_enables_feature_required_by_added_column():
    # Given an existing table without the feature required by a desired new column
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("seen_at", TimestampNtz()),
        )
    )

    # When diffing the desired and observed schemas
    diff = diff_table(desired, _observed())

    # Then feature enablement is planned before the dependent column addition
    assert isinstance(diff, TableDrift)
    assert len(diff.actions) == 2
    assert EnableTableFeature(TableFeature.TIMESTAMP_NTZ) in diff.actions
    assert AddColumn(DesiredColumn("seen_at", TimestampNtz())) in diff.actions


def test_existing_table_diff_does_not_reenable_supported_feature():
    # Given an existing table that already supports the desired column's feature
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("seen_at", TimestampNtz()),
        )
    )
    observed = _observed(supported_features=frozenset({TableFeature.TIMESTAMP_NTZ}))

    # When diffing the desired and observed schemas
    diff = diff_table(desired, observed)

    # Then only the new column is planned
    assert isinstance(diff, TableDrift)
    assert diff.actions == (AddColumn(DesiredColumn("seen_at", TimestampNtz())),)


def test_existing_table_diff_finds_features_in_nested_type_trees():
    # Given feature-requiring types nested inside a map, struct, and array
    payload = Map(
        String(),
        Struct(
            fields=(
                StructField("seen_at", Array(TimestampNtz())),
                StructField("value", Variant()),
            )
        ),
    )
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("payload", payload),
        )
    )

    # When diffing against a table without those features
    diff = diff_table(desired, _observed())

    # Then both canonical feature enablements are planned
    assert isinstance(diff, TableDrift)
    feature_actions = tuple(
        action for action in diff.actions if isinstance(action, EnableTableFeature)
    )
    assert feature_actions == (
        EnableTableFeature(TableFeature.TIMESTAMP_NTZ),
        EnableTableFeature(TableFeature.VARIANT),
    )


def test_existing_table_diff_only_enables_missing_required_features():
    # Given a schema requiring two features when the table already supports one
    desired = _desired(
        columns=(
            DesiredColumn("seen_at", TimestampNtz()),
            DesiredColumn("payload", Map(String(), Variant())),
        )
    )
    observed = _observed(
        columns=(ObservedColumn("seen_at", TimestampNtz()),),
        supported_features=frozenset({TableFeature.TIMESTAMP_NTZ}),
    )

    # When diffing the desired and observed schemas
    diff = diff_table(desired, observed)

    # Then only the missing feature is enabled
    assert isinstance(diff, TableDrift)
    feature_actions = tuple(
        action for action in diff.actions if isinstance(action, EnableTableFeature)
    )
    assert feature_actions == (EnableTableFeature(TableFeature.VARIANT),)


def test_existing_table_diff_reports_feature_gap_without_column_drift():
    # Given matching columns whose required table feature is absent
    desired = _desired(columns=(DesiredColumn("seen_at", TimestampNtz()),))
    observed = _observed(columns=(ObservedColumn("seen_at", TimestampNtz()),))

    # When diffing the otherwise identical schemas
    diff = diff_table(desired, observed)

    # Then the feature gap remains actionable drift
    assert isinstance(diff, TableDrift)
    assert diff.actions == (EnableTableFeature(TableFeature.TIMESTAMP_NTZ),)


def test_missing_table_relies_on_create_for_required_features():
    # Given a missing table declaring a feature-requiring type
    desired = _desired(columns=(DesiredColumn("seen_at", TimestampNtz()),))

    # When diffing it against absence
    diff = diff_table(desired, None)

    # Then creation relies on Delta to enable the required feature
    assert isinstance(diff, TableCreation)
    assert diff.actions == (CreateTable(desired),)


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
        SetColumnTag(column_name="id", name="new", desired_value="x", observed_value=None),
        SetColumnTag(column_name="id", name="pii", desired_value="true", observed_value="false"),
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
    assert (
        SetColumnTag(column_name="new", name="pii", desired_value="true", observed_value=None)
        in diff.actions
    )


def test_identical_column_tags_produce_no_changes():
    # Given matched columns with identical tags
    columns = (DesiredColumn("id", Integer(), tags={"pii": "true"}),)
    diff = diff_table(_desired(columns=columns), _observed(columns=columns))

    # Then no tag changes are produced
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.unresolvable == ()


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
    assert diff.unresolvable == ()


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
    assert diff.unresolvable == ()


def test_undeclared_managed_key_produces_an_undeclared_property_finding():
    # Given a managed key on the table that the declaration omits
    diff = diff_table(
        _desired(properties={}),
        _observed(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then the diff records the missing declaration intent as unresolvable
    assert isinstance(diff, TableDrift)
    assert diff.unresolvable == (
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
    assert diff.unresolvable == (
        PropertyUndeclared(name="delta.enableChangeDataFeed", observed_value="true"),
    )


def test_properties_diff_is_skipped_when_properties_unmanaged():
    # Given a declaration that does not manage properties (metadata-only style)
    # over a catalog carrying an undeclared managed key
    diff = diff_table(
        _desired(properties={}, scope=TableScope.METADATA),
        _observed(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then no property change of any kind — no assertion was made
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.unresolvable == ()


def test_declared_properties_are_not_compared_when_properties_unmanaged():
    # Given a declaration that carries a property but does not manage
    # properties, over a catalog where that property is absent
    diff = diff_table(
        _desired(properties={"delta.enableChangeDataFeed": "true"}, scope=TableScope.METADATA),
        _observed(properties={}),
    )

    # Then the carried property makes no assertion and produces no change
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.unresolvable == ()


def test_property_set_rejects_equal_values():
    with pytest.raises(ValueError):
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
        SetTableTag(name="env", desired_value="prod", observed_value=None),
        UnsetTableTag(name="stale"),
    }


def test_changed_table_tag_preserves_observed_value():
    diff = diff_table(
        _desired(tags={"env": "prod"}),
        _observed(tags={"env": "dev"}),
    )

    assert isinstance(diff, TableDrift)
    assert diff.actions == (SetTableTag(name="env", desired_value="prod", observed_value="dev"),)


# ---------- partitioning change


def test_partitioning_drift_produces_change_with_both_sides():
    # Given identical columns on both sides so partitioning is the only drift
    columns = (DesiredColumn("id", Integer()), DesiredColumn("value", Integer()))
    diff = diff_table(
        _desired(columns=columns, partitioned_by=("id",)),
        _observed(columns=columns),
    )

    assert isinstance(diff, TableDrift)
    assert diff.unresolvable == (
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
    assert diff.unresolvable == ()


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


# ---------- primary key changes


def test_desired_only_primary_key_produces_added_change():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer(), nullable=False),), primary_key=pk),
        _observed(columns=(DesiredColumn("id", Integer(), nullable=False),)),
    )

    assert isinstance(diff, TableDrift)
    assert diff.actions == (SetPrimaryKey(primary_key=pk),)


def test_equal_primary_keys_by_column_set_and_name_produce_no_change():
    # Given the same PK column set and name under different orders and casing
    desired_pk = PrimaryKeyConstraint(columns=("a", "b"), constraint_name="Other_Name")
    observed_pk = PrimaryKeyConstraint(columns=("b", "a"), constraint_name="other_name")
    columns = (
        DesiredColumn("a", Integer(), nullable=False),
        DesiredColumn("b", Integer(), nullable=False),
    )

    # When diffing the declarations
    diff = diff_table(
        _desired(columns=columns, primary_key=desired_pk),
        _observed(columns=columns, primary_key=observed_pk),
    )

    # Then column-set and identifier-name identity agree — no change
    assert isinstance(diff, TableDrift)
    assert diff.actions == ()
    assert diff.unresolvable == ()


def test_explicit_primary_key_name_drift_produces_drop_and_set_actions():
    # Given identical key columns under different physical names
    desired_pk = PrimaryKeyConstraint(columns=("id",), constraint_name="managed_pk")
    observed_pk = PrimaryKeyConstraint(columns=("id",), constraint_name="legacy_pk")
    columns = (DesiredColumn("id", Integer(), nullable=False),)

    # When diffing the declarations
    diff = diff_table(
        _desired(columns=columns, primary_key=desired_pk),
        _observed(columns=columns, primary_key=observed_pk),
    )

    # Then the observed name is replaced by the explicitly managed name
    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        DropPrimaryKey("legacy_pk"),
        SetPrimaryKey(primary_key=desired_pk),
    )


def test_unnamed_primary_key_adopts_observed_name_when_columns_match():
    desired_pk = PrimaryKeyConstraint(columns=("id",))
    observed_pk = PrimaryKeyConstraint(columns=("id",), constraint_name="databricks_generated_pk")
    columns = (DesiredColumn("id", Integer(), nullable=False),)

    diff = diff_table(
        _desired(columns=columns, primary_key=desired_pk),
        _observed(columns=columns, primary_key=observed_pk),
    )

    assert isinstance(diff, TableDrift)
    assert diff.actions == ()


# ---------- constraint changes


def test_present_table_diffs_foreign_keys_by_name_and_definition():
    # Given one FK declared-and-present, one declared-but-absent,
    # and one observed-but-undeclared
    shared = _foreign_key("orders_customer_fk", local_columns=("customer_id",))
    declared_only = _foreign_key("orders_billing_fk", local_columns=("billing_id",))
    observed_only = _foreign_key("legacy_archive_fk", local_columns=("legacy_id",))
    desired = _desired(
        columns=(
            DesiredColumn("customer_id", String()),
            DesiredColumn("billing_id", String()),
            DesiredColumn("legacy_id", String()),
        ),
        foreign_keys=(shared, declared_only),
    )
    observed = _observed(
        columns=(
            ObservedColumn("customer_id", String()),
            ObservedColumn("billing_id", String()),
            ObservedColumn("legacy_id", String()),
        ),
        foreign_keys=(shared, observed_only),
    )

    # When the table is diffed
    drift = diff_table(desired, observed)

    # Then the absent declaration is set and the undeclared observation dropped
    assert isinstance(drift, TableDrift)
    assert drift.actions == (
        DropForeignKey(constraint=observed_only),
        SetForeignKey(constraint=declared_only),
    )


def test_foreign_key_name_drift_replaces_the_constraint():
    # Given the same FK definition under different desired and observed names
    desired_key = _foreign_key("orders_customer_fk", local_columns=("customer_id",))
    observed_key = _foreign_key("legacy_customer_fk", local_columns=("customer_id",))
    columns = (DesiredColumn("customer_id", String()),)

    # When the table is diffed
    drift = diff_table(
        _desired(columns=columns, foreign_keys=(desired_key,)),
        _observed(columns=columns, foreign_keys=(observed_key,)),
    )

    # Then the observed occurrence is dropped and the desired one is set
    assert isinstance(drift, TableDrift)
    assert drift.actions == (
        DropForeignKey(constraint=observed_key),
        SetForeignKey(constraint=desired_key),
    )


def test_unnamed_foreign_key_adopts_observed_name_when_definition_matches():
    desired_key = _foreign_key(None, local_columns=("customer_id",))
    observed_key = _foreign_key("databricks_generated_fk", local_columns=("customer_id",))
    columns = (DesiredColumn("customer_id", String()),)

    drift = diff_table(
        _desired(columns=columns, foreign_keys=(desired_key,)),
        _observed(columns=columns, foreign_keys=(observed_key,)),
    )

    assert isinstance(drift, TableDrift)
    assert drift.actions == ()


def test_explicit_foreign_key_names_are_reserved_before_unnamed_keys_are_adopted():
    unnamed_customer = _foreign_key(None, local_columns=("customer_id",))
    named_billing = _foreign_key("claimed_fk", local_columns=("billing_id",))
    observed_customer = _foreign_key("claimed_fk", local_columns=("customer_id",))
    observed_billing = _foreign_key("legacy_billing_fk", local_columns=("billing_id",))
    columns = (
        DesiredColumn("customer_id", String()),
        DesiredColumn("billing_id", String()),
    )

    drift = diff_table(
        _desired(columns=columns, foreign_keys=(unnamed_customer, named_billing)),
        _observed(columns=columns, foreign_keys=(observed_customer, observed_billing)),
    )

    assert isinstance(drift, TableDrift)
    assert drift.actions == (
        DropForeignKey(constraint=observed_customer),
        DropForeignKey(constraint=observed_billing),
        SetForeignKey(constraint=unnamed_customer),
        SetForeignKey(constraint=named_billing),
    )


def test_foreign_key_name_identity_is_case_insensitive():
    # Given matching definitions whose physical-name spelling differs only by case
    desired_key = _foreign_key("Orders_Customer_FK", local_columns=("customer_id",))
    observed_key = _foreign_key("orders_customer_fk", local_columns=("customer_id",))
    columns = (DesiredColumn("customer_id", String()),)

    # When the table is diffed
    drift = diff_table(
        _desired(columns=columns, foreign_keys=(desired_key,)),
        _observed(columns=columns, foreign_keys=(observed_key,)),
    )

    # Then identifier-equivalent names converge without replacement
    assert isinstance(drift, TableDrift)
    assert drift.actions == ()


def test_foreign_key_definition_drift_under_the_same_name_replaces_the_constraint():
    # Given one physical FK name whose desired and observed definitions differ
    desired_key = _foreign_key("orders_customer_fk", local_columns=("customer_id",))
    observed_key = _foreign_key("orders_customer_fk", local_columns=("legacy_customer_id",))
    columns = (
        DesiredColumn("customer_id", String()),
        DesiredColumn("legacy_customer_id", String()),
    )

    # When the table is diffed
    drift = diff_table(
        _desired(columns=columns, foreign_keys=(desired_key,)),
        _observed(columns=columns, foreign_keys=(observed_key,)),
    )

    # Then the stale definition is replaced under that name
    assert isinstance(drift, TableDrift)
    assert drift.actions == (
        DropForeignKey(constraint=observed_key),
        SetForeignKey(constraint=desired_key),
    )


def test_set_foreign_key_carries_the_declared_spelling():
    # Given a declared FK whose catalog columns are spelled differently
    declared = _foreign_key("orders_customer_fk", local_columns=("CustomerId",))
    desired = _desired(
        columns=(DesiredColumn("CustomerId", String()),),
        foreign_keys=(declared,),
    )
    observed = _observed(columns=(ObservedColumn("customerid", String()),))

    drift = diff_table(desired, observed)

    # Then the action is a semantic value: declared spelling, untouched.
    # The case drift itself is stated separately (ColumnCaseDrift) and
    # rejected by ColumnSpellingMustMatchCatalog — stating both is the
    # differ's job; judging is validation's.
    (action,) = [a for a in drift.actions if isinstance(a, SetForeignKey)]
    assert tuple(str(c) for c in action.constraint.local_columns) == ("CustomerId",)


def test_missing_table_actions_include_every_declared_foreign_key():
    # Given a missing table declaring an outbound FK and a self-referential FK
    outbound = _foreign_key("orders_customer_fk", local_columns=("customer_id",))
    self_ref = _foreign_key(
        "orders_parent_fk",
        local_columns=("parent_order_id",),
        referenced_columns=("id",),
        referenced_table=_QUALIFIED_NAME,
    )
    desired = _desired(
        columns=(
            DesiredColumn("id", String(), nullable=False),
            DesiredColumn("customer_id", String()),
            DesiredColumn("parent_order_id", String()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(outbound, self_ref),
    )

    diff = diff_table(desired, observed=None)

    # Then creation is stated first and every declared FK follows
    assert isinstance(diff, TableCreation)
    assert isinstance(diff.actions[0], CreateTable)
    fk_actions = [a for a in diff.actions if isinstance(a, SetForeignKey)]
    assert {str(a.constraint.constraint_name) for a in fk_actions} == {
        "orders_customer_fk",
        "orders_parent_fk",
    }


def test_foreign_key_drift_is_stated_even_when_unmanaged():
    # Given a declaration that does not manage foreign keys but declares one
    declared = _foreign_key("orders_customer_fk", local_columns=("customer_id",))
    desired = _desired(
        columns=(DesiredColumn("customer_id", String()),),
        foreign_keys=(declared,),
        scope=TableScope.ANNOTATIONS,
    )
    observed = _observed(columns=(ObservedColumn("customer_id", String()),))

    drift = diff_table(desired, observed)

    # Then the difference is stated; rejecting it is the eligibility check's job
    assert any(isinstance(a, SetForeignKey) for a in drift.actions)


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
    assert diff.actions == (DropPrimaryKey("legacy_pk"),)


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
        DropPrimaryKey("legacy_pk"),
        SetPrimaryKey(primary_key=desired_primary_key),
    )


def test_set_primary_key_carries_the_declared_spelling():
    # Given a PK declared camelCase over a column the catalog spells lowercase
    desired = _desired(
        columns=(DesiredColumn("orderId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("orderId",), constraint_name="orders_pk"),
    )
    observed = _observed(columns=(ObservedColumn("orderid", String(), nullable=False),))

    # When the table is diffed
    drift = diff_table(desired, observed)

    # Then the action is a semantic value carrying the declaration verbatim;
    # the case drift itself is stated separately and rejected by validation
    set_actions = [a for a in drift.actions if isinstance(a, SetPrimaryKey)]
    assert len(set_actions) == 1
    assert tuple(str(c) for c in set_actions[0].primary_key.columns) == ("orderId",)
    assert any(isinstance(u, ColumnCaseDrift) for u in drift.unresolvable)


def test_set_primary_key_keeps_declared_spelling_for_new_columns():
    # Given a PK over a column that does not exist in the catalog yet
    desired = _desired(
        columns=(DesiredColumn("orderId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("orderId",), constraint_name="orders_pk"),
    )
    observed = _observed(columns=(ObservedColumn("other", String(), nullable=False),))

    # When the table is diffed
    drift = diff_table(desired, observed)

    # Then the new column keeps its declared spelling
    set_actions = [a for a in drift.actions if isinstance(a, SetPrimaryKey)]
    assert len(set_actions) == 1
    assert tuple(str(c) for c in set_actions[0].primary_key.columns) == ("orderId",)


def test_observed_only_column_tags_are_unset_before_the_column_is_removed():
    # Given an observed-only column that also has catalog tags
    observed_only_column = ObservedColumn("stale", Integer(), tags={"old": "true"})

    # When diffing
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer()),)),
        _observed(columns=(DesiredColumn("id", Integer()), observed_only_column)),
    )

    # Then removing the column also states the prerequisite tag cleanup
    assert isinstance(diff, TableDrift)
    assert diff.actions == (
        UnsetColumnTag(column_name="stale", name="old"),
        DropColumn(observed_only_column),
    )


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
    assert drift.unresolvable == ()


def test_diff_hint_is_a_plain_add_when_neither_name_is_observed():
    # Given a rename hint whose source column was never observed
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("customer_name", String(), renamed_from="customer_nm"),
        )
    )
    observed = _observed(columns=(DesiredColumn("id", Integer()),))

    actions = diff_table(desired, observed).actions

    # Then the hint is inert and the column is a plain add
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

    # The conflict is unresolvable, and the source column is neither dropped nor renamed
    assert drift.unresolvable == (
        ColumnRenameConflict(old_name="customer_nm", new_name="customer_name"),
    )
    assert drift.actions == ()


def test_diff_missing_table_ignores_rename_hints():
    desired = _desired(
        columns=(DesiredColumn("customer_name", String(), renamed_from="customer_nm"),)
    )

    diff = diff_table(desired, None)

    assert isinstance(diff, TableCreation)
    assert diff.desired is desired


def test_diff_projects_clustering_identity_across_a_rename():
    # Given clustering keyed by a column that is being renamed
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

    # Then identity projects across the rename — only the rename itself remains
    assert actions == (RenameColumn(old_name="customer_nm", new_name="customer_name"),)


def test_diff_projects_partition_identity_across_a_rename():
    # Given partitioning keyed by a column that is being renamed
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

    # Then identity projects across the rename — only the rename itself remains
    assert actions == (RenameColumn(old_name="day", new_name="event_day"),)


def test_diff_rename_and_primary_key_replacement_are_direct_actions():
    # Given a primary key moving to a renamed column
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

    # Then the rename plus an explicit key drop-and-set are direct actions
    assert set(drift.actions) == {
        RenameColumn(old_name="customer_nm", new_name="customer_name"),
        DropPrimaryKey("legacy_pk"),
        SetPrimaryKey(primary_key=desired_key),
    }


def test_drift_carries_the_observed_table_it_was_computed_against():
    # The drift's endpoints are judging context: validation's eligibility checks
    # read observed facts (the relation kind) off the observed side.
    qualified_name = QualifiedName("cat", "sch", "clicks")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("id", Integer()),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("id", Integer()),),
        kind=TableKind.STREAMING_TABLE,
    )

    diff = diff_table(desired, observed)

    assert isinstance(diff, TableDrift)
    assert diff.observed is observed
    assert diff.observed.kind is TableKind.STREAMING_TABLE


def test_drift_against_an_ordinary_table_carries_the_table_kind():
    qualified_name = QualifiedName("cat", "sch", "orders")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("id", Integer()),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("id", Integer()),),
    )

    diff = diff_table(desired, observed)

    assert isinstance(diff, TableDrift)
    assert diff.observed.kind is TableKind.TABLE


def test_case_only_column_difference_produces_no_actions_but_states_the_drift():
    # A case variant is the same column, so nothing is actionable — but the
    # spelling disagreement itself is stated for validation to judge.
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("requestid", String()),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("requestId", String()),),
    )

    diff = diff_table(desired, observed)

    assert diff.actions == ()
    assert diff.unresolvable == (ColumnCaseDrift("requestid", "requestId"),)


def test_case_only_layout_and_key_differences_produce_no_actions():
    # Layout and key identities are case-insensitive, so no constraint or
    # clustering action fires; the one stated difference is the column's own
    # spelling disagreement.
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("requestId", String(), nullable=False),),
        clustered_by=("requestId",),
        primary_key=PrimaryKeyConstraint(columns=("requestId",), constraint_name="t_pk"),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("requestid", String(), nullable=False),),
        clustered_by=("requestid",),
        primary_key=PrimaryKeyConstraint(columns=("requestid",), constraint_name="t_pk"),
    )

    diff = diff_table(desired, observed)

    assert diff.actions == ()
    assert diff.unresolvable == (ColumnCaseDrift("requestId", "requestid"),)


def test_case_only_struct_field_difference_is_not_drift():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("payload", Struct((StructField("requestId", String()),))),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("payload", Struct((StructField("requestid", String()),))),),
    )

    assert diff_table(desired, observed).actions == ()


def test_genuinely_different_name_still_reports_structural_drift():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("requestId", String()),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("request_id", String()),),
    )

    diff = diff_table(desired, observed)

    action_types = {type(action) for action in diff.actions}
    assert action_types == {AddColumn, DropColumn}


def test_matched_column_action_uses_observed_spelling_across_casing():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("requestid", String(), comment="AWS request id"),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("requestId", String(), comment=""),),
    )

    [action] = diff_table(desired, observed).actions

    assert isinstance(action, SetColumnComment)
    assert str(action.column_name) == "requestId"


def test_rename_source_uses_observed_spelling_when_hint_casing_differs():
    qualified_name = QualifiedName("cat", "sch", "t")
    desired = DesiredTable(
        qualified_name=qualified_name,
        columns=(DesiredColumn("newName", String(), renamed_from="oldname"),),
    )
    observed = ObservedTable(
        qualified_name=qualified_name,
        columns=(ObservedColumn("OldName", String()),),
    )

    [action] = diff_table(desired, observed).actions

    assert isinstance(action, RenameColumn)
    assert str(action.old_name) == "OldName"
    assert str(action.new_name) == "newName"


# ---------- column case drift


def test_column_case_drift_requires_a_real_spelling_difference():
    with pytest.raises(ValueError):
        ColumnCaseDrift(declared_name="orderid", observed_name="orderid")


def test_column_case_drift_requires_the_same_identifier():
    with pytest.raises(ValueError):
        ColumnCaseDrift(declared_name="orderid", observed_name="customer_id")


def test_column_case_drift_equality_is_exact_even_when_built_from_identifiers():
    # Given a drift built from Identifier names, as the differ builds them
    drift = ColumnCaseDrift(Identifier("OrderId"), Identifier("orderid"))

    # Then the record compares by exact spelling, not case-insensitively
    assert drift == ColumnCaseDrift("OrderId", "orderid")
    assert drift != ColumnCaseDrift("ORDERID", "orderid")


def test_matched_column_case_drift_is_stated_as_unresolvable():
    # Given a declared column whose catalog counterpart is spelled differently
    desired = _desired(columns=(DesiredColumn("OrderId", String()),))
    observed = _observed(columns=(ObservedColumn("orderid", String()),))

    # When the table is diffed
    drift = diff_table(desired, observed)

    # Then the disagreement is stated with both spellings, verbatim
    assert isinstance(drift, TableDrift)
    drifts = [u for u in drift.unresolvable if isinstance(u, ColumnCaseDrift)]
    assert [(d.declared_name, d.observed_name) for d in drifts] == [("OrderId", "orderid")]


def test_agreeing_column_case_states_no_drift():
    # Given declared and observed spellings that agree exactly
    desired = _desired(columns=(DesiredColumn("order_id", String()),))
    observed = _observed(columns=(ObservedColumn("order_id", String()),))

    drift = diff_table(desired, observed)

    assert not any(isinstance(u, ColumnCaseDrift) for u in drift.unresolvable)


def test_renamed_from_with_wrong_case_is_stated_as_case_drift():
    # Given a rename hint spelling the catalog column differently
    desired = _desired(columns=(DesiredColumn("newName", String(), renamed_from="oldname"),))
    observed = _observed(columns=(ObservedColumn("OldName", String()),))

    drift = diff_table(desired, observed)

    # Then the hint's reference to the existing column is flagged; the rename
    # action itself still carries the observed spelling (differ mechanics)
    drifts = [u for u in drift.unresolvable if isinstance(u, ColumnCaseDrift)]
    assert [(d.declared_name, d.observed_name) for d in drifts] == [("oldname", "OldName")]


def test_rename_with_exact_source_spelling_states_no_drift():
    # Given a rename whose hint matches the catalog spelling exactly —
    # the target is a new name and new names are spelled freely
    desired = _desired(columns=(DesiredColumn("newName", String(), renamed_from="OldName"),))
    observed = _observed(columns=(ObservedColumn("OldName", String()),))

    drift = diff_table(desired, observed)

    assert not any(isinstance(u, ColumnCaseDrift) for u in drift.unresolvable)
