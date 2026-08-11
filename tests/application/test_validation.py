from typing import ClassVar

import pytest

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import (
    DEFAULT_SAFETY_RULES,
    ELIGIBILITY_CHECKS,
    UnmanagedAspectDrift,
    validate_diff,
)
from delta_engine.domain.model import (
    Byte,
    DataType,
    Date,
    Decimal,
    DesiredColumn,
    DesiredTable,
    Double,
    Float,
    ForeignKeyReference,
    Integer,
    Long,
    ObservedColumn,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
    Short,
    String,
    Struct,
    StructField,
    TableKind,
    TableScope,
    TimestampNtz,
)
from delta_engine.domain.plan import (
    Action,
    AddColumn,
    AlterColumnType,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnTag,
    SetTableComment,
    Unresolvable,
)
from delta_engine.domain.plan.diff import (
    TableDrift,
    diff_table,
)
from delta_engine.domain.plan.unresolvable import (
    ColumnCaseDrift,
    ColumnRenameConflict,
    PartitioningChanged,
)
from tests.builders import as_observed_columns

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _desired_table(
    *,
    columns: tuple[DesiredColumn, ...] | None = None,
    properties: dict[str, str | None] | None = None,
    partitioned_by: tuple[str, ...] = (),
    clustered_by: tuple[str, ...] = (),
    scope: TableScope = TableScope.FULL,
    primary_key: PrimaryKeyConstraint | None = None,
) -> DesiredTable:
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(DesiredColumn("id", Integer()),) if columns is None else columns,
        properties={} if properties is None else properties,
        partitioned_by=partitioned_by,
        clustered_by=clustered_by,
        scope=scope,
        primary_key=primary_key,
    )


def _observed_table(
    *,
    columns: tuple[DesiredColumn, ...] | None = None,
    properties: dict[str, str] | None = None,
    partitioned_by: tuple[str, ...] = (),
    clustered_by: tuple[str, ...] = (),
    kind: TableKind = TableKind.TABLE,
    referencing_foreign_keys: tuple[ForeignKeyReference, ...] = (),
    primary_key: PrimaryKeyConstraint | None = None,
) -> ObservedTable:
    source = (DesiredColumn("id", Integer()),) if columns is None else columns
    return ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=as_observed_columns(source),
        properties={} if properties is None else properties,
        partitioned_by=partitioned_by,
        clustered_by=clustered_by,
        kind=kind,
        referencing_foreign_keys=referencing_foreign_keys,
        primary_key=primary_key,
    )


def _drift(
    *differences: Action | Unresolvable,
    scope: TableScope = TableScope.FULL,
    desired: DesiredTable | None = None,
    observed: ObservedTable | None = None,
    kind: TableKind = TableKind.TABLE,
) -> TableDrift:
    if desired is None:
        desired = _desired_table(scope=scope)
    if observed is None:
        observed = _observed_table(kind=kind)
    actions = tuple(item for item in differences if isinstance(item, Action))
    unresolvable = tuple(item for item in differences if not isinstance(item, Action))
    return TableDrift(
        desired=desired,
        observed=observed,
        actions=actions,
        unresolvable=unresolvable,
    )


def _validate(
    desired: DesiredTable,
    observed: ObservedTable | None,
    *,
    rules=DEFAULT_SAFETY_RULES,
) -> tuple[ValidationFailure, ...]:
    return validate_diff(diff_table(desired, observed), rules=rules)


def _type_drift(column_name: str = "id") -> AlterColumnType:
    return AlterColumnType(
        column_name=column_name,
        desired_type=Long(),
        observed_type=Integer(),
    )


# ---- validation composition


def test_eligibility_checks_cover_all_laws_in_evaluation_order():
    check_names = tuple(type(check).__name__ for check in ELIGIBILITY_CHECKS)

    assert check_names == (
        "ColumnSpellingMustMatchCatalog",
        "MissingTableUnmanaged",
        "StreamingTableAnnotationsOnly",
        "UnmanagedAspectDrift",
    )


def test_default_rules_cover_all_safety_policies():
    # Given the DEFAULT_SAFETY_RULES constant — eligibility checks are not default rules
    rule_names = tuple(type(rule).__name__ for rule in DEFAULT_SAFETY_RULES)

    # Then the expected safety policies are enabled in deterministic evaluation order
    assert rule_names == (
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
        "NonWideningColumnTypeChange",
        "TypeWideningRequiredForTypeChange",
        "PartitioningChangeNotSupported",
        "PropertyTransitionNotSupported",
        "PropertyMustBeDeclared",
        "ColumnMappingRequiredForDrop",
        "AmbiguousColumnRename",
        "PrimaryKeyReferencedByForeignKeys",
    )


def test_validate_diff_uses_default_rules_when_rules_are_not_supplied():
    # Given a diff that violates a default safety rule
    desired = _desired_table(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("x", Integer(), nullable=False),
        )
    )
    observed = _observed_table(columns=(DesiredColumn("id", Integer()),))

    # When validating without explicit rules
    failures = _validate(desired, observed)

    # Then the default rules are applied
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_validate_diff_allows_safety_rules_to_be_suppressed():
    # Given a diff that would normally violate a default safety rule
    desired = _desired_table(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("x", Integer(), nullable=False),
        )
    )
    observed = _observed_table(columns=(DesiredColumn("id", Integer()),))

    # When validating with no safety rules
    failures = _validate(desired, observed, rules=())

    # Then safety-rule failures are suppressed
    assert not failures


def test_validate_diff_applies_supplied_rules_to_drift():
    # Given a custom rule supplied by the caller
    class AlwaysFail:
        name: ClassVar[str] = "AlwaysFail"

        def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
            return (
                ValidationFailure(
                    rule_name=self.name,
                    message=f"checked {drift.desired.qualified_name}",
                ),
            )

    # When validating a harmless drift with that rule
    failures = validate_diff(_drift(), rules=(AlwaysFail(),))

    # Then the custom rule contributes its failure
    assert failures == (
        ValidationFailure(rule_name="AlwaysFail", message="checked dev.silver.test"),
    )


# ---- passing validation


def test_validation_passes_when_no_rule_is_broken():
    # Given a drift containing only a nullable column addition
    desired = _desired_table(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("age", Integer()),
        )
    )
    observed = _observed_table(columns=(DesiredColumn("id", Integer()),))

    # When validating the diff
    failures = _validate(desired, observed)

    # Then validation passes
    assert not failures


def test_empty_drift_produces_no_failures():
    # Given an empty drift
    failures = validate_diff(_drift())

    # Then validation passes
    assert not failures


# ---- missing table validation


def test_missing_table_with_non_nullable_columns_passes_when_table_existence_is_managed():
    # Given a missing table with a NOT NULL column
    desired = _desired_table(
        columns=(DesiredColumn("id", Integer(), nullable=False),),
        scope=TableScope.FULL,
    )

    # Then creating the table is safe
    assert not validate_diff(diff_table(desired, None))


def test_validate_diff_fails_table_missing_when_table_existence_unmanaged():
    # Given a metadata-only definition for a table that does not exist
    desired = _desired_table(
        scope=TableScope.ANNOTATIONS,
    )

    # When validating
    failures = validate_diff(diff_table(desired, None))

    # Then the missing table cannot be created
    assert failures[0].rule_name == "MissingTableUnmanaged"
    assert "does not exist" in failures[0].message


def test_validate_diff_passes_table_missing_when_table_existence_managed():
    # Given a fully managed definition for a missing table
    desired = _desired_table(scope=TableScope.FULL)

    # When validating
    failures = validate_diff(diff_table(desired, None))

    # Then creation is allowed
    assert not failures


def test_missing_table_unmanaged_cannot_be_suppressed_by_empty_rules():
    # Given a missing table whose declaration does not manage table existence
    desired = _desired_table(
        scope=TableScope.ANNOTATIONS,
    )

    # When validating with no safety rules
    failures = validate_diff(diff_table(desired, None), rules=())

    # Then the eligibility check still fails
    assert failures[0].rule_name == "MissingTableUnmanaged"


# ---- non-nullable column additions


def test_rejects_adding_non_nullable_columns_to_existing_table():
    # Given an existing table and a declaration adding two NOT NULL columns
    desired = _desired_table(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("a", Integer(), nullable=False),
            DesiredColumn("b", String(), nullable=False),
        )
    )
    observed = _observed_table(columns=(DesiredColumn("id", Integer()),))

    # When validating the diff
    failures = _validate(desired, observed)

    # Then each unsafe column addition is reported
    assert [failure.rule_name for failure in failures] == [
        "NonNullableColumnAdd",
        "NonNullableColumnAdd",
    ]
    assert "a" in failures[0].message
    assert "b" in failures[1].message
    assert [failure.subject for failure in failures] == ["a", "b"]
    assert "backfill" in failures[0].message
    assert "nullable=False" in failures[0].message


def test_allows_adding_nullable_column_to_existing_table():
    # Given an existing table and a declaration adding a nullable column
    desired = _desired_table(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("age", Integer()),
        ),
    )
    observed = _observed_table(columns=(DesiredColumn("id", Integer()),))

    # Then validation passes
    assert not _validate(desired, observed)


# ---- nullability changes


def test_rejects_tightening_existing_columns_to_not_null():
    # Given existing nullable columns tightened to NOT NULL
    desired = _desired_table(
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("email", String(), nullable=False),
        )
    )
    observed = _observed_table(
        columns=(
            DesiredColumn("id", Integer(), nullable=True),
            DesiredColumn("email", String(), nullable=True),
        )
    )

    # When validating the diff
    failures = _validate(desired, observed)

    # Then each tightening is rejected
    assert [failure.rule_name for failure in failures] == [
        "NullabilityTighteningOnExistingColumn",
        "NullabilityTighteningOnExistingColumn",
    ]
    assert "id" in failures[0].message
    assert "email" in failures[1].message
    assert [failure.subject for failure in failures] == ["id", "email"]


def test_allows_loosening_existing_column_to_nullable():
    # Given an existing NOT NULL column and a declaration loosening it
    desired = _desired_table(columns=(DesiredColumn("id", Integer(), nullable=True),))
    observed = _observed_table(columns=(DesiredColumn("id", Integer(), nullable=False),))

    # Then validation passes
    assert not _validate(desired, observed)


# ---- unsupported structural changes


def test_rejects_widening_type_change_without_type_widening_declared():
    # Given an existing column widened Integer → Long, with no property declared
    desired = _desired_table(columns=(DesiredColumn("id", Long()),))
    observed = _observed_table(columns=(DesiredColumn("id", Integer()),))

    # When validating the diff
    failures = _validate(desired, observed)

    # Then the widen is rejected pending the enabling property
    assert len(failures) == 1
    assert failures[0].rule_name == "TypeWideningRequiredForTypeChange"
    assert "id" in failures[0].message
    assert "delta.enableTypeWidening" in failures[0].message
    assert failures[0].subject == "id"


def test_widening_type_change_passes_with_type_widening_declared():
    # Given a widen Integer → Long with the property declared in the same sync
    desired = _desired_table(
        columns=(DesiredColumn("id", Long()),),
        properties={"delta.enableTypeWidening": "true"},
    )
    observed = _observed_table(columns=(DesiredColumn("id", Integer()),))

    # When validating
    failures = _validate(desired, observed)

    # Then the widen is permitted (SET_PROPERTY phases before ALTER_COLUMN_TYPE)
    assert not failures


@pytest.mark.parametrize(
    ("column_name", "desired_type", "observed_type"),
    [
        ("id", String(), Integer()),
        (
            "payload",
            Struct((StructField("code", String(), nullable=False),)),
            Struct((StructField("code", String(), nullable=True),)),
        ),
    ],
    ids=["scalar-type", "struct-field-nullability"],
)
def test_rejects_non_widening_type_change_even_with_type_widening_declared(
    column_name, desired_type, observed_type
):
    # Given a modeled type change that Delta cannot widen in place
    desired = _desired_table(
        columns=(DesiredColumn(column_name, desired_type),),
        properties={"delta.enableTypeWidening": "true"},
    )
    observed = _observed_table(columns=(DesiredColumn(column_name, observed_type),))

    failures = _validate(desired, observed)

    [failure] = failures
    assert failure.rule_name == "NonWideningColumnTypeChange"
    assert "recreate the table" in failure.message
    assert failure.subject == column_name


def test_rejects_narrowing_type_change():
    # Given Long → Integer — the reverse of a widening is a narrowing
    desired = _desired_table(
        columns=(DesiredColumn("id", Integer()),),
        properties={"delta.enableTypeWidening": "true"},
    )
    observed = _observed_table(columns=(DesiredColumn("id", Long()),))

    failures = _validate(desired, observed)

    assert failures[0].rule_name == "NonWideningColumnTypeChange"


def _widening_failures(
    desired_type: DataType, observed_type: DataType
) -> tuple[ValidationFailure, ...]:
    """Validate a single-column type change with type widening declared."""
    desired = _desired_table(
        columns=(DesiredColumn("c", desired_type),),
        properties={"delta.enableTypeWidening": "true"},
    )
    observed = _observed_table(columns=(DesiredColumn("c", observed_type),))
    return _validate(desired, observed)


def test_decimal_widening_keeps_integer_digits_and_never_shrinks_scale():
    # Then precision growth at unchanged scale passes
    assert not _widening_failures(Decimal(12, 2), Decimal(10, 2))
    # And scale growth passes when precision grows with it (integer digits kept)
    assert not _widening_failures(Decimal(12, 3), Decimal(10, 1))
    # And scale growth that eats into integer digits is blocked
    assert _widening_failures(Decimal(10, 3), Decimal(10, 2))[0].rule_name == (
        "NonWideningColumnTypeChange"
    )
    # And a precision shrink is blocked
    assert _widening_failures(Decimal(8, 2), Decimal(10, 2))[0].rule_name == (
        "NonWideningColumnTypeChange"
    )
    # And a scale shrink is blocked even though integer digits grow
    assert _widening_failures(Decimal(12, 1), Decimal(10, 2))[0].rule_name == (
        "NonWideningColumnTypeChange"
    )


def test_integer_to_decimal_widening_requires_enough_integer_digits():
    # Given Databricks' minimums: DECIMAL(10,0) for Byte/Short/Integer, DECIMAL(20,0) for Long
    assert not _widening_failures(Decimal(10, 0), Integer())
    assert not _widening_failures(Decimal(12, 2), Byte())
    assert not _widening_failures(Decimal(20, 0), Long())

    # Then a decimal without room for every source value is blocked
    assert _widening_failures(Decimal(9, 0), Integer())[0].rule_name == (
        "NonWideningColumnTypeChange"
    )
    assert _widening_failures(Decimal(11, 2), Short())[0].rule_name == (
        "NonWideningColumnTypeChange"
    )
    assert _widening_failures(Decimal(19, 0), Long())[0].rule_name == (
        "NonWideningColumnTypeChange"
    )


def test_long_cannot_widen_to_double():
    # Given Long → Double — absent from the Delta matrix (Double cannot hold every Long)
    assert _widening_failures(Double(), Long())[0].rule_name == "NonWideningColumnTypeChange"


def test_every_widening_matrix_entry_is_permitted():
    # Given each matrix entry against a declaration with widening enabled
    cases = (
        (Short(), Byte()),
        (Integer(), Byte()),
        (Long(), Byte()),
        (Double(), Byte()),
        (Integer(), Short()),
        (Long(), Short()),
        (Double(), Short()),
        (Long(), Integer()),
        (Double(), Integer()),
        (Double(), Float()),
        (TimestampNtz(), Date()),
    )
    for desired_type, observed_type in cases:
        assert not _widening_failures(desired_type, observed_type), (
            observed_type,
            desired_type,
        )


def test_rejects_partitioning_change():
    # Given a declaration that changes table partitioning (identical columns on
    # both sides so partitioning is the only drift)
    columns = (DesiredColumn("id", Integer()), DesiredColumn("value", Integer()))
    desired = _desired_table(columns=columns, partitioned_by=("id",))
    observed = _observed_table(columns=columns)

    # When validating the diff
    failures = _validate(desired, observed)

    # Then the partitioning change is rejected
    assert len(failures) == 1
    assert failures[0].rule_name == "PartitioningChangeNotSupported"


def test_allows_clustering_change():
    # Given a declaration that changes clustering (identical columns on both
    # sides so clustering is the only drift). Unlike partitioning, liquid
    # clustering keys are reconciled in place, so no safety rule blocks them.
    columns = (DesiredColumn("id", Integer()), DesiredColumn("region", String()))
    desired = _desired_table(columns=columns, clustered_by=("region",))
    observed = _observed_table(columns=columns)

    # When validating the diff
    failures = _validate(desired, observed)

    # Then it passes — clustering is an allowed in-place change
    assert not failures


def test_validate_diff_collects_both_unsupported_drift_and_rule_failures():
    # Given a drift with a type change and a NOT NULL column addition
    diff = _drift(
        _type_drift("id"),
        AddColumn(DesiredColumn("new_col", Integer(), nullable=False)),
    )

    # When validating
    failures = validate_diff(diff)

    # Then both failures are returned in one pass
    assert [failure.rule_name for failure in failures] == [
        "NonNullableColumnAdd",
        "TypeWideningRequiredForTypeChange",
    ]


# ---- unmanaged aspect drift


def test_unmanaged_aspect_drift_fails_when_unmanaged_aspect_has_drifted():
    # Given a declaration that only manages table tags, but column structure has drifted
    diff = _drift(
        AddColumn(DesiredColumn("extra", Integer())),
        scope=TableScope.TAGS,
    )

    # When validating
    failures = validate_diff(diff)

    # Then one failure names the unmanaged aspect
    assert len(failures) == 1
    assert failures[0].rule_name == "UnmanagedAspectDrift"
    assert "column structure" in failures[0].message.lower()


def test_unmanaged_aspect_drift_produces_one_failure_per_drifted_unmanaged_aspect():
    # Given two changes in one unmanaged aspect and one change in another
    diff = _drift(
        AddColumn(DesiredColumn("extra", Integer())),
        AddColumn(DesiredColumn("more", Integer())),
        SetTableComment(desired_comment="new", observed_comment="old"),
        scope=TableScope.TAGS,
    )

    # When validating
    failures = validate_diff(diff)

    # Then one failure is produced per aspect, not per change
    assert len(failures) == 2
    assert "column structure" in failures[0].message.lower()
    assert "table comment" in failures[1].message.lower()


def test_unmanaged_aspect_drift_cannot_be_suppressed_by_empty_rules():
    # Given unmanaged drift and an empty rule set
    diff = _drift(
        AddColumn(DesiredColumn("extra", Integer())),
        scope=TableScope.TAGS,
    )

    # When validating
    failures = validate_diff(diff, rules=())

    # Then the eligibility check still fires
    assert failures[0].rule_name == "UnmanagedAspectDrift"


def test_unmanaged_drift_does_not_also_trip_safety_rules():
    # Given a metadata-only declaration whose live table has a type mismatch
    diff = _drift(
        _type_drift("id"),
        scope=TableScope.ANNOTATIONS,
    )

    # When validating
    failures = validate_diff(diff)

    # Then the single failure is the scope violation, not the type-change rule
    assert len(failures) == 1
    assert failures[0].rule_name == "UnmanagedAspectDrift"


def test_managed_drift_still_trips_safety_rules():
    # Given a fully managed drift with a type change
    diff = _drift(_type_drift("id"), scope=TableScope.FULL)

    # When validating
    failures = validate_diff(diff)

    # Then the safety rule fires
    assert len(failures) == 1
    assert failures[0].rule_name == "TypeWideningRequiredForTypeChange"


def test_drift_passes_when_no_unmanaged_aspect_has_drifted():
    # Given a metadata-only drift where only a managed aspect drifted
    diff = _drift(
        SetTableComment(desired_comment="new", observed_comment="old"),
        scope=TableScope.ANNOTATIONS,
    )

    # Then validation passes
    assert not validate_diff(diff)


def test_drift_passes_when_all_aspects_managed():
    # Given a fully managed drift with a nullable column addition
    diff = _drift(AddColumn(DesiredColumn("extra", Integer())), scope=TableScope.FULL)

    # Then validation passes
    assert not validate_diff(diff)


def test_unmanaged_column_drop_does_not_require_column_mapping():
    # Given column structure drift on a declaration that does not manage column structure
    diff = _drift(
        DropColumn(ObservedColumn("stale", Integer())),
        scope=TableScope.ANNOTATIONS,
    )

    # When validating
    failures = validate_diff(diff)

    # Then validation reports only the scope failure, not the drop precondition
    assert [failure.rule_name for failure in failures] == ["UnmanagedAspectDrift"]


def test_tag_only_scope_passes_when_only_table_and_column_tags_drift():
    # Given a tag-only declaration with table and column tag drift
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(DesiredColumn("id", Integer(), tags={"pii": "false"}),),
        tags={"domain": "sales"},
        scope=TableScope.TAGS,
    )
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(ObservedColumn("id", Integer(), tags={"pii": "true"}),),
        tags={"legacy": "yes"},
    )

    # When validating the diff
    failures = _validate(desired, observed)

    # Then both tag aspects are managed, so the drift is allowed
    assert not failures


def test_tag_only_scope_fails_when_table_comment_drifts():
    # Given a tag-only declaration with comment drift
    diff = _drift(
        SetTableComment(desired_comment="new", observed_comment="old"),
        scope=TableScope.TAGS,
    )

    # When validating
    failures = validate_diff(diff)

    # Then table comments are outside this declaration's responsibility
    assert failures[0].rule_name == "UnmanagedAspectDrift"
    assert "table comment" in failures[0].message


def test_tag_only_scope_fails_when_column_comment_drifts():
    # Given a tag-only declaration with column comment drift
    diff = _drift(
        SetColumnTag(column_name="id", name="pii", desired_value="true", observed_value=None),
        SetColumnComment(column_name="id", desired_comment="new", observed_comment="old"),
        scope=TableScope.TAGS,
    )

    # When validating
    failures = validate_diff(diff)

    # Then managed tag drift does not hide unmanaged comment drift
    assert [failure.rule_name for failure in failures] == ["UnmanagedAspectDrift"]


def test_tag_only_scope_fails_when_column_structure_drifts():
    # Given a tag-only declaration whose live table has an extra, undeclared column
    desired = _desired_table(
        columns=(DesiredColumn("id", Integer()),),
        scope=TableScope.TAGS,
    )
    observed = _observed_table(
        columns=(DesiredColumn("id", Integer()), DesiredColumn("extra", String()))
    )

    # When validating the diff
    failures = _validate(desired, observed)

    # Then unmanaged column-structure drift fails before any tag SQL runs
    assert [failure.rule_name for failure in failures] == ["UnmanagedAspectDrift"]


# ---- property transition rules


def test_blocks_column_mapping_downgrade():
    # Given a declaration downgrading column mapping from name to none
    failures = _validate(
        _desired_table(properties={"delta.columnMapping.mode": "none"}),
        _observed_table(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then validation rejects the transition
    assert failures[0].rule_name == "PropertyTransitionNotSupported"
    assert "delta.columnMapping.mode" in failures[0].message
    assert failures[0].subject == "delta.columnMapping.mode"


def test_allows_column_mapping_upgrade():
    # Given the permitted transition from none to name
    failures = _validate(
        _desired_table(properties={"delta.columnMapping.mode": "name"}),
        _observed_table(properties={"delta.columnMapping.mode": "none"}),
    )

    # Then validation passes
    assert not failures


def test_allows_first_write_of_restricted_key():
    # Given the restricted key is absent from the catalog
    failures = _validate(
        _desired_table(properties={"delta.columnMapping.mode": "name"}),
        _observed_table(properties={}),
    )

    # Then validation passes
    assert not failures


def test_ignores_value_changes_on_unrestricted_keys():
    # Given a value change on a property with no restricted transitions
    failures = _validate(
        _desired_table(properties={"delta.enableChangeDataFeed": "false"}),
        _observed_table(properties={"delta.enableChangeDataFeed": "true"}),
    )

    # Then validation passes
    assert not failures


def test_blocks_none_declaration_on_removal_forbidden_key():
    # Given a declaration trying to remove columnMapping.mode
    failures = _validate(
        _desired_table(properties={"delta.columnMapping.mode": None}),
        _observed_table(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then validation rejects the removal
    assert failures[0].rule_name == "PropertyTransitionNotSupported"
    assert "cannot be removed" in failures[0].message
    assert failures[0].subject == "delta.columnMapping.mode"


def test_allows_none_declaration_on_unrestricted_key():
    # Given an absence assertion on an unrestricted key
    failures = _validate(
        _desired_table(properties={"delta.logRetentionDuration": None}),
        _observed_table(properties={"delta.logRetentionDuration": "interval 30 days"}),
    )

    # Then validation passes
    assert not failures


# ---- undeclared property rules


def test_fails_undeclared_unrestricted_property_and_suggests_none():
    # Given an observed managed property missing from the declaration
    failures = _validate(
        _desired_table(properties={}),
        _observed_table(properties={"delta.enableChangeDataFeed": "true"}),
    )

    # Then validation tells the user to declare it or declare None
    assert failures[0].rule_name == "PropertyMustBeDeclared"
    assert "None" in failures[0].message
    assert failures[0].subject == "delta.enableChangeDataFeed"


def test_fails_undeclared_removal_forbidden_property_without_suggesting_none():
    # Given columnMapping.mode is observed but missing from the declaration
    failures = _validate(
        _desired_table(properties={}),
        _observed_table(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then validation fails without suggesting an impossible None declaration
    assert failures[0].rule_name == "PropertyMustBeDeclared"
    assert "cannot be unset" in failures[0].message
    assert "None" not in failures[0].message


# ---- column-drop precondition


def test_drop_without_column_mapping_fails_before_execution():
    # Given a plan that drops a column but the declaration lacks column mapping
    diff = _drift(DropColumn(ObservedColumn("stale", Integer())))

    # When validating
    failures = validate_diff(diff)

    # Then the drop precondition fails
    assert any(
        failure.rule_name == "ColumnMappingRequiredForDrop"
        and "delta.columnMapping.mode" in failure.message
        for failure in failures
    )


def test_drop_with_declared_column_mapping_passes():
    # Given the declaration states mode=name
    desired = _desired_table(properties={"delta.columnMapping.mode": "name"})
    diff = _drift(DropColumn(ObservedColumn("stale", Integer())), desired=desired)

    # Then the column drop precondition passes
    assert not validate_diff(diff)


def test_multiple_column_drops_produce_one_column_mapping_failure():
    # Given multiple dropped columns but no column mapping declaration
    diff = _drift(
        DropColumn(ObservedColumn("stale_a", Integer())),
        DropColumn(ObservedColumn("stale_b", Integer())),
    )

    # When validating
    failures = validate_diff(diff)

    # Then the precondition is reported once for the table
    assert [failure.rule_name for failure in failures] == ["ColumnMappingRequiredForDrop"]


# ---- primary key referenced by foreign keys


def test_primary_key_drop_blocked_while_foreign_keys_reference_it():
    # Given a PK removal while the observed table is referenced by another table's FK
    reference = ForeignKeyReference(
        name="orders_customer_id_fk",
        referencing_table=QualifiedName("dev", "silver", "orders"),
    )
    change = DropPrimaryKey("test_pk")

    failures = validate_diff(
        _drift(change, observed=_observed_table(referencing_foreign_keys=(reference,)))
    )

    # Then validation fails naming the referencing constraint
    assert any(
        failure.rule_name == "PrimaryKeyReferencedByForeignKeys"
        and "orders_customer_id_fk" in failure.message
        for failure in failures
    )


def test_primary_key_drop_allowed_when_no_foreign_keys_reference_it():
    change = DropPrimaryKey("test_pk")

    failures = validate_diff(_drift(change))

    assert not failures


def test_primary_key_drop_allowed_when_same_sync_drops_the_referencing_fk_on_this_table():
    # Given a self-referential FK dropped in the same sync as the PK
    # (DROP_FOREIGN_KEY phases before DROP_PRIMARY_KEY, so execution succeeds)
    reference = ForeignKeyReference(
        name="test_parent_id_fk",
        referencing_table=_QUALIFIED_NAME,
    )
    pk_change = DropPrimaryKey("test_pk")
    fk_change = DropForeignKey(name="test_parent_id_fk")

    observed = _observed_table(referencing_foreign_keys=(reference,))
    failures = validate_diff(_drift(pk_change, fk_change, observed=observed))

    assert not any(failure.rule_name == "PrimaryKeyReferencedByForeignKeys" for failure in failures)


# ---- AmbiguousColumnRename


def test_ambiguous_rename_fails_when_source_and_target_both_exist():
    desired = _desired_table(
        columns=(DesiredColumn("customer_name", String(), renamed_from="customer_nm"),),
        properties={"delta.columnMapping.mode": "name"},
    )
    drift = TableDrift(
        desired=desired,
        observed=_observed_table(),
        unresolvable=(ColumnRenameConflict(old_name="customer_nm", new_name="customer_name"),),
    )

    failures = validate_diff(drift)

    rename_failures = [f for f in failures if f.rule_name == "AmbiguousColumnRename"]
    assert len(rename_failures) == 1
    message = rename_failures[0].message
    assert "customer_nm" in message and "customer_name" in message
    assert rename_failures[0].subject == "customer_nm"


def test_removed_column_that_is_not_a_rename_source_is_not_ambiguous():
    desired = _desired_table(
        columns=(DesiredColumn("id", Integer(), nullable=False),),
        properties={"delta.columnMapping.mode": "name"},
    )
    drift = TableDrift(
        desired=desired,
        observed=_observed_table(),
        actions=(DropColumn(column=ObservedColumn("old", String())),),
    )

    failures = validate_diff(drift)

    assert not any(f.rule_name == "AmbiguousColumnRename" for f in failures)


# ---- ColumnSpellingMustMatchCatalog


def test_column_case_drift_fails_validation_naming_both_spellings():
    # Given a diff stating a column spelled differently from the catalog
    drift = TableDrift(
        desired=_desired_table(columns=(DesiredColumn("OrderId", String()),)),
        observed=_observed_table(columns=(ObservedColumn("orderid", String()),)),
        unresolvable=(ColumnCaseDrift(declared_name="OrderId", observed_name="orderid"),),
    )

    # When the diff is validated
    failures = validate_diff(drift)

    # Then the rejection names the rule and both spellings
    spelling_failures = [f for f in failures if f.rule_name == "ColumnSpellingMustMatchCatalog"]
    assert len(spelling_failures) == 1
    assert "'OrderId'" in spelling_failures[0].message
    assert "'orderid'" in spelling_failures[0].message
    assert spelling_failures[0].subject == "OrderId"


def test_agreeing_spelling_passes_the_case_rule():
    # Given declared and observed spellings that agree exactly, end to end
    desired = _desired_table(columns=(DesiredColumn("order_id", String()),))
    observed = _observed_table(columns=(ObservedColumn("order_id", String()),))

    failures = validate_diff(diff_table(desired, observed))

    assert not any(f.rule_name == "ColumnSpellingMustMatchCatalog" for f in failures)


def test_column_case_drift_is_named_at_a_scope_that_does_not_manage_columns():
    # Given a metadata-scoped declaration whose only difference is a column
    # spelled differently from the catalog — the shape of a declaration adding
    # keys or comments to a table someone else created
    drift = _drift(
        ColumnCaseDrift(declared_name="requestid", observed_name="requestId"),
        scope=TableScope.METADATA,
    )

    # When the diff is validated
    failures = validate_diff(drift)

    # Then the spelling itself is named. A misspelled reference is a defect in
    # the declaration, not drift in an aspect the declaration may decline to
    # manage, so it does not hide behind UnmanagedAspectDrift
    assert [failure.rule_name for failure in failures] == ["ColumnSpellingMustMatchCatalog"]


def test_column_spelling_gate_cannot_be_suppressed_by_empty_rules():
    # Given case drift and no safety rules at all
    drift = _drift(ColumnCaseDrift(declared_name="requestid", observed_name="requestId"))

    failures = validate_diff(drift, rules=())

    # Then exact spelling still holds — it is a law, not a suppressible policy
    assert [failure.rule_name for failure in failures] == ["ColumnSpellingMustMatchCatalog"]


def test_column_spelling_gate_short_circuits_safety_rules():
    # Given a misspelled column alongside a change a safety rule would reject
    drift = _drift(
        ColumnCaseDrift(declared_name="requestid", observed_name="requestId"),
        _type_drift("id"),
    )

    failures = validate_diff(drift)

    # Then the spelling is reported alone: a diff whose column references
    # disagree with the catalog is not worth safety judgement yet
    assert [failure.rule_name for failure in failures] == ["ColumnSpellingMustMatchCatalog"]


def test_column_spelling_check_reports_before_unmanaged_aspect_drift():
    # Given a metadata-scoped declaration with both a misspelled column and
    # genuinely unmanaged structural drift, so two eligibility checks fire
    drift = _drift(
        ColumnCaseDrift(declared_name="requestid", observed_name="requestId"),
        AddColumn(DesiredColumn("extra", Integer())),
        scope=TableScope.METADATA,
    )

    failures = validate_diff(drift)

    # Then the spelling failure leads: it is the actionable root defect, and a
    # reader who fixes the spelling first re-reads a trustworthy diff
    assert [failure.rule_name for failure in failures] == [
        "ColumnSpellingMustMatchCatalog",
        "UnmanagedAspectDrift",
    ]


def test_column_spelling_check_reports_before_the_streaming_table_check():
    # Given a streaming table whose declaration both misspells a column and
    # claims more than annotations. UnmanagedAspectDrift stays silent — the case
    # drift is the one difference it passes over — so this isolates the spelling
    # and kind checks against each other
    diff = _drift(
        ColumnCaseDrift(declared_name="requestid", observed_name="requestId"),
        scope=TableScope.METADATA,
        kind=TableKind.STREAMING_TABLE,
    )

    failures = validate_diff(diff)

    # Then the spelling leads. Narrowing the scope is what the kind failure asks
    # for, and it would not make the misspelling right — so the defect that
    # survives the suggested fix is reported first
    assert [failure.rule_name for failure in failures] == [
        "ColumnSpellingMustMatchCatalog",
        "StreamingTableAnnotationsOnly",
    ]


# ---- streaming tables


_PIPELINE_KEY = PrimaryKeyConstraint(("id",), "test_pk")
# Unity Catalog will not key a nullable column and the engine will not declare
# one, so the key column of a pipeline-declared key is NOT NULL on both sides
_KEYED_COLUMNS = (DesiredColumn("id", Integer(), nullable=False),)


@pytest.mark.parametrize("scope", [TableScope.TAGS, TableScope.ANNOTATIONS])
def test_streaming_table_admits_a_declaration_within_annotations(scope):
    # Given a declaration claiming no more than comments and tags — the aspects
    # a pipeline does not own
    diff = _drift(scope=scope, kind=TableKind.STREAMING_TABLE)

    # Then the engine may manage them
    assert not validate_diff(diff)


@pytest.mark.parametrize("scope", [TableScope.METADATA, TableScope.FULL])
def test_streaming_table_refuses_a_declaration_beyond_annotations(scope):
    # Given a declaration claiming keys as well, over an in-sync table — zero
    # drift, so there is nothing to do either way
    diff = _drift(scope=scope, kind=TableKind.STREAMING_TABLE)

    # When validating
    failures = validate_diff(diff)

    # Then it is rejected on the claim alone rather than on any difference, so a
    # dry run surfaces the misdeclaration before the table has drifted at all
    assert [failure.rule_name for failure in failures] == ["StreamingTableAnnotationsOnly"]
    assert 'scope="annotations"' in failures[0].message


def test_streaming_table_check_reports_before_unmanaged_aspect_drift():
    # Given a metadata scope with structure drift on a streaming table —
    # both eligibility checks fire, kind first
    diff = _drift(
        AddColumn(DesiredColumn("extra", Integer())),
        scope=TableScope.METADATA,
        kind=TableKind.STREAMING_TABLE,
    )

    failures = validate_diff(diff)

    assert [failure.rule_name for failure in failures] == [
        "StreamingTableAnnotationsOnly",
        "UnmanagedAspectDrift",
    ]


def test_annotations_scope_refuses_to_drop_a_pipeline_declared_key():
    # Given a streaming table whose key was declared in the pipeline's defining
    # SQL, and an annotations-scope declaration that does not restate it
    desired = _desired_table(columns=_KEYED_COLUMNS, scope=TableScope.ANNOTATIONS)
    observed = _observed_table(
        columns=_KEYED_COLUMNS, kind=TableKind.STREAMING_TABLE, primary_key=_PIPELINE_KEY
    )

    # When validating the real diff of the two
    failures = _validate(desired, observed)

    # Then the kind check stays silent — annotations is a scope it admits — and
    # the omitted key reads as a drop this declaration has no authority to make
    assert [failure.rule_name for failure in failures] == ["UnmanagedAspectDrift"]
    assert "primary key" in failures[0].message.lower()


def test_a_declaration_mirroring_the_pipeline_key_leaves_it_alone():
    # Given the same table, with the declaration restating the pipeline's key
    desired = _desired_table(
        columns=_KEYED_COLUMNS, scope=TableScope.ANNOTATIONS, primary_key=_PIPELINE_KEY
    )
    observed = _observed_table(
        columns=_KEYED_COLUMNS, kind=TableKind.STREAMING_TABLE, primary_key=_PIPELINE_KEY
    )

    # Then the key yields no action to disown. Mirroring is what the engine asks
    # of a declaration over a pipeline-owned table, and it costs the declaration
    # nothing: the key stays out of the managed aspects either way
    assert not _validate(desired, observed)


def test_scope_failure_prevents_safety_evaluation():
    class MustNotRun:
        name: ClassVar[str] = "MustNotRun"

        def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
            raise AssertionError("safety rule ran after scope rejection")

    diff = _drift(
        AddColumn(DesiredColumn("extra", Integer())),
        scope=TableScope.TAGS,
    )

    failures = validate_diff(diff, rules=(MustNotRun(),))

    assert failures[0].rule_name == "UnmanagedAspectDrift"


def test_unmanaged_drift_names_the_differences_that_drifted():
    # Given a metadata-scoped declaration against a table whose columns drifted
    diff = _drift(
        AddColumn(DesiredColumn("legacy_region", String())),
        AlterColumnType(column_name="amount", desired_type=Long(), observed_type=Integer()),
        scope=TableScope.METADATA,
    )

    # When the eligibility check judges it
    (failure,) = UnmanagedAspectDrift().evaluate(diff)

    # Then the message names the aspect, and each difference is stated in full
    assert "column structure" in failure.message
    assert failure.details == (
        "+ legacy_region String",
        "~ amount Integer → Long",
    )


def test_unmanaged_drift_names_differences_no_action_could_close():
    # Given drift a plan action cannot express
    diff = _drift(
        PartitioningChanged(desired_partitioning=("region",), observed_partitioning=("country",)),
        scope=TableScope.METADATA,
    )

    # When the check judges it
    (failure,) = UnmanagedAspectDrift().evaluate(diff)

    # Then it is named too — an unresolvable difference is often the whole reason
    assert failure.details == ("~ partitioning (country) → (region)",)


def test_unmanaged_drift_reports_one_failure_per_aspect():
    # Given drift in two unmanaged aspects at once
    diff = _drift(
        AddColumn(DesiredColumn("legacy_region", String())),
        PartitioningChanged(desired_partitioning=("region",), observed_partitioning=("country",)),
        scope=TableScope.METADATA,
    )

    # When the check judges it
    failures = UnmanagedAspectDrift().evaluate(diff)

    # Then each aspect gets its own failure, each naming only its own differences
    assert len(failures) == 2
    assert [failure.subject for failure in failures] == ["column structure", "partitioning"]
    assert [failure.details for failure in failures] == [
        ("+ legacy_region String",),
        ("~ partitioning (country) → (region)",),
    ]


def test_unmanaged_drift_still_passes_over_a_column_spelled_differently():
    # Given case drift, which ColumnSpellingMustMatchCatalog owns at every scope
    diff = _drift(
        ColumnCaseDrift(declared_name="SKU", observed_name="sku"),
        scope=TableScope.METADATA,
    )

    # Then this check says nothing about it
    assert UnmanagedAspectDrift().evaluate(diff) == ()
