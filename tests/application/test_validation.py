from typing import ClassVar

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import (
    DEFAULT_RULES,
    ValidationResult,
    validate_diff,
)
from delta_engine.domain.model import (
    ALL_ASPECTS,
    Column,
    DesiredTable,
    ForeignKeyConstraint,
    ForeignKeyReference,
    Integer,
    Long,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
    TableAspect,
)
from delta_engine.domain.plan.changes import (
    Change,
    ColumnAdded,
    ColumnCommentChanged,
    ColumnDataTypeChanged,
    ColumnRemoved,
    ColumnTagSet,
    ForeignKeyRemoved,
    PrimaryKeyChanged,
    PrimaryKeyRemoved,
    TableCommentChanged,
)
from delta_engine.domain.plan.diff import (
    TableDrift,
    TableMissing,
    diff_table,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _desired_table(
    *,
    columns: tuple[Column, ...] | None = None,
    properties: dict[str, str | None] | None = None,
    partitioned_by: tuple[str, ...] = (),
    managed_aspects: frozenset[TableAspect] = ALL_ASPECTS,
) -> DesiredTable:
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),) if columns is None else columns,
        properties={} if properties is None else properties,
        partitioned_by=partitioned_by,
        managed_aspects=managed_aspects,
    )


def _observed_table(
    *,
    columns: tuple[Column, ...] | None = None,
    properties: dict[str, str] | None = None,
    partitioned_by: tuple[str, ...] = (),
) -> ObservedTable:
    return ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),) if columns is None else columns,
        properties={} if properties is None else properties,
        partitioned_by=partitioned_by,
    )


def _drift(
    *changes: Change,
    managed_aspects: frozenset[TableAspect] = ALL_ASPECTS,
    desired: DesiredTable | None = None,
) -> TableDrift:
    if desired is None:
        desired = _desired_table(managed_aspects=managed_aspects)
    return TableDrift(desired=desired, changes=tuple(changes))


def _validate(
    desired: DesiredTable,
    observed: ObservedTable | None,
    *,
    rules=DEFAULT_RULES,
) -> ValidationResult:
    return validate_diff(diff_table(desired, observed), rules=rules)


def _type_drift(column_name: str = "id") -> ColumnDataTypeChanged:
    return ColumnDataTypeChanged(
        column_name=column_name,
        desired_type=Long(),
        observed_type=Integer(),
    )


# ---- ValidationResult


def test_validation_result_failed_property():
    # Given a result with one failure and a result with no failures
    failing = ValidationResult(failures=(ValidationFailure(rule_name="X", message="broken"),))
    passing = ValidationResult()

    # Then failed reflects whether any failures are present
    assert failing.failed is True
    assert passing.failed is False


# ---- DEFAULT_RULES


def test_default_rules_cover_all_safety_policies():
    # Given the DEFAULT_RULES constant — scope invariants are not default rules
    rule_names = {type(rule).__name__ for rule in DEFAULT_RULES}

    # Then the expected safety policies are enabled by default
    assert rule_names == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
        "ColumnDataTypeChangeNotSupported",
        "PartitioningChangeNotSupported",
        "PropertyTransitionNotSupported",
        "PropertyMustBeDeclared",
        "ColumnMappingRequiredForDrop",
        "PrimaryKeyReferencedByForeignKeys",
    }


def test_validate_diff_uses_default_rules_when_rules_are_not_supplied():
    # Given a diff that violates a default safety rule
    desired = _desired_table(
        columns=(
            Column("id", Integer()),
            Column("x", Integer(), nullable=False),
        )
    )
    observed = _observed_table(columns=(Column("id", Integer()),))

    # When validating without explicit rules
    result = _validate(desired, observed)

    # Then the default rules are applied
    assert result.failed is True
    assert result.failures[0].rule_name == "NonNullableColumnAdd"


def test_validate_diff_allows_safety_rules_to_be_suppressed():
    # Given a diff that would normally violate a default safety rule
    desired = _desired_table(
        columns=(
            Column("id", Integer()),
            Column("x", Integer(), nullable=False),
        )
    )
    observed = _observed_table(columns=(Column("id", Integer()),))

    # When validating with no safety rules
    result = _validate(desired, observed, rules=())

    # Then safety-rule failures are suppressed
    assert result.failed is False


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
    result = validate_diff(_drift(), rules=(AlwaysFail(),))

    # Then the custom rule contributes its failure
    assert result.failed is True
    assert result.failures == (
        ValidationFailure(rule_name="AlwaysFail", message="checked dev.silver.test"),
    )


# ---- passing validation


def test_validation_passes_when_no_rule_is_broken():
    # Given a drift containing only a nullable column addition
    desired = _desired_table(
        columns=(
            Column("id", Integer()),
            Column("age", Integer()),
        )
    )
    observed = _observed_table(columns=(Column("id", Integer()),))

    # When validating the diff
    result = _validate(desired, observed)

    # Then validation passes
    assert result.failed is False
    assert result.failures == ()


def test_empty_drift_produces_no_failures():
    # Given an empty drift
    result = validate_diff(_drift())

    # Then validation passes
    assert result.failed is False
    assert result.failures == ()


# ---- missing table validation


def test_missing_table_with_non_nullable_columns_passes_when_column_structure_is_managed():
    # Given a missing table with a NOT NULL column
    desired = _desired_table(
        columns=(Column("id", Integer(), nullable=False),),
        managed_aspects=ALL_ASPECTS,
    )

    # Then creating the table is safe
    assert validate_diff(TableMissing(desired=desired)).failed is False


def test_validate_diff_fails_table_missing_when_column_structure_unmanaged():
    # Given a metadata-only definition for a table that does not exist
    desired = _desired_table(
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT, TableAspect.TABLE_TAGS})
    )

    # When validating
    result = validate_diff(TableMissing(desired=desired))

    # Then the missing table cannot be created
    assert result.failed is True
    assert result.failures[0].rule_name == "MissingTableUnmanaged"
    assert "does not exist" in result.failures[0].message


def test_validate_diff_passes_table_missing_when_column_structure_managed():
    # Given a fully managed definition for a missing table
    desired = _desired_table(managed_aspects=ALL_ASPECTS)

    # When validating
    result = validate_diff(TableMissing(desired=desired))

    # Then creation is allowed
    assert result.failed is False
    assert result.failures == ()


def test_missing_table_unmanaged_cannot_be_suppressed_by_empty_rules():
    # Given a missing table whose declaration does not manage column structure
    desired = _desired_table(
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT, TableAspect.TABLE_TAGS})
    )

    # When validating with no safety rules
    result = validate_diff(TableMissing(desired=desired), rules=())

    # Then the scope invariant still fails
    assert result.failed is True
    assert result.failures[0].rule_name == "MissingTableUnmanaged"


# ---- non-nullable column additions


def test_rejects_adding_non_nullable_columns_to_existing_table():
    # Given an existing table and a declaration adding two NOT NULL columns
    desired = _desired_table(
        columns=(
            Column("id", Integer()),
            Column("a", Integer(), nullable=False),
            Column("b", String(), nullable=False),
        )
    )
    observed = _observed_table(columns=(Column("id", Integer()),))

    # When validating the diff
    result = _validate(desired, observed)

    # Then each unsafe column addition is reported
    assert result.failed is True
    assert [failure.rule_name for failure in result.failures] == [
        "NonNullableColumnAdd",
        "NonNullableColumnAdd",
    ]
    assert "a" in result.failures[0].message
    assert "b" in result.failures[1].message


def test_allows_adding_nullable_column_to_existing_table():
    # Given an existing table and a declaration adding a nullable column
    desired = _desired_table(
        columns=(
            Column("id", Integer()),
            Column("age", Integer()),
        ),
    )
    observed = _observed_table(columns=(Column("id", Integer()),))

    # Then validation passes
    assert _validate(desired, observed).failed is False


# ---- nullability changes


def test_rejects_tightening_existing_columns_to_not_null():
    # Given existing nullable columns and a declaration tightening them to NOT NULL
    desired = _desired_table(
        columns=(
            Column("id", Integer(), nullable=False),
            Column("email", String(), nullable=False),
        )
    )
    observed = _observed_table(
        columns=(
            Column("id", Integer(), nullable=True),
            Column("email", String(), nullable=True),
        )
    )

    # When validating the diff
    result = _validate(desired, observed)

    # Then each tightening is rejected
    assert result.failed is True
    assert [failure.rule_name for failure in result.failures] == [
        "NullabilityTighteningOnExistingColumn",
        "NullabilityTighteningOnExistingColumn",
    ]
    assert "id" in result.failures[0].message
    assert "email" in result.failures[1].message


def test_allows_loosening_existing_column_to_nullable():
    # Given an existing NOT NULL column and a declaration loosening it
    desired = _desired_table(columns=(Column("id", Integer(), nullable=True),))
    observed = _observed_table(columns=(Column("id", Integer(), nullable=False),))

    # Then validation passes
    assert _validate(desired, observed).failed is False


# ---- unsupported structural changes


def test_rejects_existing_column_type_change():
    # Given an existing column whose declared type changes
    desired = _desired_table(columns=(Column("id", Long()),))
    observed = _observed_table(columns=(Column("id", Integer()),))

    # When validating the diff
    result = _validate(desired, observed)

    # Then the type change is rejected
    assert result.failed is True
    assert len(result.failures) == 1
    assert result.failures[0].rule_name == "ColumnDataTypeChangeNotSupported"
    assert "id" in result.failures[0].message


def test_rejects_partitioning_change():
    # Given a declaration that changes table partitioning (identical columns on
    # both sides so partitioning is the only drift)
    columns = (Column("id", Integer()), Column("value", Integer()))
    desired = _desired_table(columns=columns, partitioned_by=("id",))
    observed = _observed_table(columns=columns)

    # When validating the diff
    result = _validate(desired, observed)

    # Then the partitioning change is rejected
    assert result.failed is True
    assert len(result.failures) == 1
    assert result.failures[0].rule_name == "PartitioningChangeNotSupported"


def test_validate_diff_collects_both_unsupported_drift_and_rule_failures():
    # Given a drift with a type change and a NOT NULL column addition
    diff = _drift(
        _type_drift("id"),
        ColumnAdded(Column("new_col", Integer(), nullable=False)),
    )

    # When validating
    result = validate_diff(diff)

    # Then both failures are returned in one pass
    assert result.failed is True
    assert [failure.rule_name for failure in result.failures] == [
        "NonNullableColumnAdd",
        "ColumnDataTypeChangeNotSupported",
    ]


# ---- unmanaged aspect drift


def test_unmanaged_aspect_drift_fails_when_unmanaged_aspect_has_drifted():
    # Given a declaration that only manages table tags, but column structure has drifted
    diff = _drift(
        ColumnAdded(Column("extra", Integer())),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS}),
    )

    # When validating
    result = validate_diff(diff)

    # Then one failure names the unmanaged aspect
    assert len(result.failures) == 1
    assert result.failures[0].rule_name == "UnmanagedAspectDrift"
    assert "column structure" in result.failures[0].message.lower()


def test_unmanaged_aspect_drift_produces_one_failure_per_drifted_unmanaged_aspect():
    # Given two changes in one unmanaged aspect and one change in another
    diff = _drift(
        ColumnAdded(Column("extra", Integer())),
        ColumnAdded(Column("more", Integer())),
        TableCommentChanged(desired_comment="new", observed_comment="old"),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS}),
    )

    # When validating
    result = validate_diff(diff)

    # Then one failure is produced per aspect, not per change
    assert len(result.failures) == 2
    assert "column structure" in result.failures[0].message.lower()
    assert "table comment" in result.failures[1].message.lower()


def test_unmanaged_aspect_drift_cannot_be_suppressed_by_empty_rules():
    # Given unmanaged drift and an empty rule set
    diff = _drift(
        ColumnAdded(Column("extra", Integer())),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS}),
    )

    # When validating
    result = validate_diff(diff, rules=())

    # Then the scope invariant still fires
    assert result.failed is True
    assert result.failures[0].rule_name == "UnmanagedAspectDrift"


def test_unmanaged_drift_does_not_also_trip_safety_rules():
    # Given a metadata-only declaration whose live table has a type mismatch
    diff = _drift(
        _type_drift("id"),
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT}),
    )

    # When validating
    result = validate_diff(diff)

    # Then the single failure is the scope violation, not the type-change rule
    assert len(result.failures) == 1
    assert result.failures[0].rule_name == "UnmanagedAspectDrift"


def test_managed_drift_still_trips_safety_rules():
    # Given a fully managed drift with a type change
    diff = _drift(_type_drift("id"), managed_aspects=ALL_ASPECTS)

    # When validating
    result = validate_diff(diff)

    # Then the safety rule fires
    assert len(result.failures) == 1
    assert result.failures[0].rule_name == "ColumnDataTypeChangeNotSupported"


def test_drift_passes_when_no_unmanaged_aspect_has_drifted():
    # Given a metadata-only drift where only a managed aspect drifted
    diff = _drift(
        TableCommentChanged(desired_comment="new", observed_comment="old"),
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT}),
    )

    # Then validation passes
    assert validate_diff(diff).failed is False


def test_drift_passes_when_all_aspects_managed():
    # Given a fully managed drift with a nullable column addition
    diff = _drift(ColumnAdded(Column("extra", Integer())), managed_aspects=ALL_ASPECTS)

    # Then validation passes
    assert validate_diff(diff).failed is False


def test_unmanaged_column_drop_does_not_require_column_mapping():
    # Given column structure drift on a declaration that does not manage column structure
    diff = _drift(
        ColumnRemoved(Column("stale", Integer())),
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT}),
    )

    # When validating
    result = validate_diff(diff)

    # Then validation reports only the scope failure, not the drop precondition
    assert result.failed is True
    assert [failure.rule_name for failure in result.failures] == ["UnmanagedAspectDrift"]


def test_tag_only_scope_passes_when_only_table_and_column_tags_drift():
    # Given a tag-only declaration with table and column tag drift
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), tags={"pii": "false"}),),
        tags={"domain": "sales"},
        managed_aspects=frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}),
    )
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), tags={"pii": "true"}),),
        tags={"legacy": "yes"},
    )

    # When validating the diff
    result = _validate(desired, observed)

    # Then both tag aspects are managed, so the drift is allowed
    assert result.failed is False


def test_tag_only_scope_fails_when_table_comment_drifts():
    # Given a tag-only declaration with comment drift
    diff = _drift(
        TableCommentChanged(desired_comment="new", observed_comment="old"),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}),
    )

    # When validating
    result = validate_diff(diff)

    # Then table comments are outside this declaration's responsibility
    assert result.failed is True
    assert result.failures[0].rule_name == "UnmanagedAspectDrift"
    assert "table comment" in result.failures[0].message


def test_tag_only_scope_fails_when_column_comment_drifts():
    # Given a tag-only declaration with column comment drift
    diff = _drift(
        ColumnTagSet(column_name="id", tag_name="pii", tag_value="true"),
        ColumnCommentChanged(column_name="id", desired_comment="new", observed_comment="old"),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}),
    )

    # When validating
    result = validate_diff(diff)

    # Then managed tag drift does not hide unmanaged comment drift
    assert result.failed is True
    assert [failure.rule_name for failure in result.failures] == ["UnmanagedAspectDrift"]


def test_tag_only_scope_fails_when_column_structure_drifts():
    # Given a tag-only declaration whose live table has an extra, undeclared column
    desired = _desired_table(
        columns=(Column("id", Integer()),),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}),
    )
    observed = _observed_table(columns=(Column("id", Integer()), Column("extra", String())))

    # When validating the diff
    result = _validate(desired, observed)

    # Then unmanaged column-structure drift fails before any tag SQL runs
    assert result.failed is True
    assert [failure.rule_name for failure in result.failures] == ["UnmanagedAspectDrift"]


# ---- property transition rules


def test_blocks_column_mapping_downgrade():
    # Given a declaration downgrading column mapping from name to none
    result = _validate(
        _desired_table(properties={"delta.columnMapping.mode": "none"}),
        _observed_table(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then validation rejects the transition
    assert result.failed is True
    assert result.failures[0].rule_name == "PropertyTransitionNotSupported"
    assert "delta.columnMapping.mode" in result.failures[0].message


def test_allows_column_mapping_upgrade():
    # Given the permitted transition from none to name
    result = _validate(
        _desired_table(properties={"delta.columnMapping.mode": "name"}),
        _observed_table(properties={"delta.columnMapping.mode": "none"}),
    )

    # Then validation passes
    assert result.failed is False


def test_allows_first_write_of_restricted_key():
    # Given the restricted key is absent from the catalog
    result = _validate(
        _desired_table(properties={"delta.columnMapping.mode": "name"}),
        _observed_table(properties={}),
    )

    # Then validation passes
    assert result.failed is False


def test_ignores_value_changes_on_unrestricted_keys():
    # Given a value change on a property with no restricted transitions
    result = _validate(
        _desired_table(properties={"delta.enableChangeDataFeed": "false"}),
        _observed_table(properties={"delta.enableChangeDataFeed": "true"}),
    )

    # Then validation passes
    assert result.failed is False


def test_blocks_none_declaration_on_removal_forbidden_key():
    # Given a declaration trying to remove columnMapping.mode
    result = _validate(
        _desired_table(properties={"delta.columnMapping.mode": None}),
        _observed_table(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then validation rejects the removal
    assert result.failed is True
    assert result.failures[0].rule_name == "PropertyTransitionNotSupported"
    assert "cannot be removed" in result.failures[0].message


def test_allows_none_declaration_on_unrestricted_key():
    # Given an absence assertion on an unrestricted key
    result = _validate(
        _desired_table(properties={"delta.logRetentionDuration": None}),
        _observed_table(properties={"delta.logRetentionDuration": "interval 30 days"}),
    )

    # Then validation passes
    assert result.failed is False


# ---- undeclared property rules


def test_fails_undeclared_unrestricted_property_and_suggests_none():
    # Given an observed managed property missing from the declaration
    result = _validate(
        _desired_table(properties={}),
        _observed_table(properties={"delta.enableChangeDataFeed": "true"}),
    )

    # Then validation tells the user to declare it or declare None
    assert result.failed is True
    assert result.failures[0].rule_name == "PropertyMustBeDeclared"
    assert "None" in result.failures[0].message


def test_fails_undeclared_removal_forbidden_property_without_suggesting_none():
    # Given columnMapping.mode is observed but missing from the declaration
    result = _validate(
        _desired_table(properties={}),
        _observed_table(properties={"delta.columnMapping.mode": "name"}),
    )

    # Then validation fails without suggesting an impossible None declaration
    assert result.failed is True
    assert result.failures[0].rule_name == "PropertyMustBeDeclared"
    assert "cannot be unset" in result.failures[0].message
    assert "None" not in result.failures[0].message


# ---- column-drop precondition


def test_drop_without_column_mapping_fails_before_execution():
    # Given a plan that drops a column but the declaration lacks column mapping
    diff = _drift(ColumnRemoved(Column("stale", Integer())))

    # When validating
    result = validate_diff(diff)

    # Then the drop precondition fails
    assert any(
        failure.rule_name == "ColumnMappingRequiredForDrop"
        and "delta.columnMapping.mode" in failure.message
        for failure in result.failures
    )


def test_drop_with_declared_column_mapping_passes():
    # Given the declaration states mode=name
    desired = _desired_table(properties={"delta.columnMapping.mode": "name"})
    diff = _drift(ColumnRemoved(Column("stale", Integer())), desired=desired)

    # Then the column drop precondition passes
    assert validate_diff(diff).failed is False


def test_multiple_column_drops_produce_one_column_mapping_failure():
    # Given multiple dropped columns but no column mapping declaration
    diff = _drift(
        ColumnRemoved(Column("stale_a", Integer())),
        ColumnRemoved(Column("stale_b", Integer())),
    )

    # When validating
    result = validate_diff(diff)

    # Then the precondition is reported once for the table
    assert [failure.rule_name for failure in result.failures] == ["ColumnMappingRequiredForDrop"]


# ---- primary key referenced by foreign keys


def test_primary_key_drop_blocked_while_foreign_keys_reference_it():
    # Given a PK removal observed to be referenced by another table's FK
    reference = ForeignKeyReference(
        constraint_name="orders_customer_id_fk",
        referencing_table=QualifiedName("dev", "silver", "orders"),
    )
    change = PrimaryKeyRemoved(
        observed_primary_key=PrimaryKeyConstraint(("id",), "customers_pk"),
        referencing_foreign_keys=(reference,),
    )

    result = validate_diff(_drift(change))

    # Then validation fails naming the referencing constraint
    assert result.failed
    assert any(
        failure.rule_name == "PrimaryKeyReferencedByForeignKeys"
        and "orders_customer_id_fk" in failure.message
        for failure in result.failures
    )


def test_primary_key_drop_allowed_when_no_foreign_keys_reference_it():
    change = PrimaryKeyRemoved(
        observed_primary_key=PrimaryKeyConstraint(("id",), "customers_pk"),
        referencing_foreign_keys=(),
    )

    result = validate_diff(_drift(change))

    assert not result.failed


def test_primary_key_drop_allowed_when_same_sync_drops_the_referencing_fk_on_this_table():
    # Given a self-referential FK dropped in the same sync as the PK
    # (DROP_FOREIGN_KEY phases before DROP_PRIMARY_KEY, so execution succeeds)
    own_fk = ForeignKeyConstraint(
        local_columns=("parent_id",),
        referenced_table=_QUALIFIED_NAME,
        referenced_columns=("id",),
        constraint_name="test_parent_id_fk",
    )
    reference = ForeignKeyReference(
        constraint_name="test_parent_id_fk",
        referencing_table=_QUALIFIED_NAME,
    )
    pk_change = PrimaryKeyRemoved(
        observed_primary_key=PrimaryKeyConstraint(("id",), "test_pk"),
        referencing_foreign_keys=(reference,),
    )
    fk_change = ForeignKeyRemoved(constraint=own_fk)

    result = validate_diff(_drift(pk_change, fk_change))

    assert not any(
        failure.rule_name == "PrimaryKeyReferencedByForeignKeys" for failure in result.failures
    )


def test_primary_key_change_blocked_while_foreign_keys_reference_it():
    reference = ForeignKeyReference(
        constraint_name="orders_customer_id_fk",
        referencing_table=QualifiedName("dev", "silver", "orders"),
    )
    change = PrimaryKeyChanged(
        desired_primary_key=PrimaryKeyConstraint(("id", "region"), "customers_pk"),
        observed_primary_key=PrimaryKeyConstraint(("id",), "customers_pk"),
        referencing_foreign_keys=(reference,),
    )

    result = validate_diff(_drift(change))

    assert result.failed
