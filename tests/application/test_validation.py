from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import (
    DEFAULT_RULES,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    UnmanagedDimensionDrift,
    ValidationResult,
    validate_diff,
)
from delta_engine.domain.model import ALL_ASPECTS, Column, DesiredTable, Integer, Long, QualifiedName, String
from delta_engine.domain.model import TableAspect
from delta_engine.domain.plan.diff import (
    Changed,
    ColumnAdded,
    ColumnCommentChanged,
    ColumnCommentsDimension,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnStructureDimension,
    PartitioningDimension,
    TableCommentDimension,
    TableDrift,
    TableMissing,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _desired_table(managed_aspects: frozenset[TableAspect] = ALL_ASPECTS) -> DesiredTable:
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        managed_aspects=managed_aspects,
    )


def _tightening(column_name: str = "id") -> ColumnNullabilityChanged:
    return ColumnNullabilityChanged(
        column_name=column_name, change=Changed(desired=False, observed=True)
    )


def _type_drift(column_name: str = "id") -> ColumnDataTypeChanged:
    return ColumnDataTypeChanged(
        column_name=column_name, change=Changed(desired=Long(), observed=Integer())
    )


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column():
    # Given a dimensions tuple containing a columns dimension with a NOT NULL addition
    rule = NonNullableColumnAdd()
    dimensions = (
        ColumnStructureDimension(entries=(ColumnAdded(Column("order_id", Integer(), nullable=False)),)),
    )

    # When
    failures = rule.evaluate(dimensions, _desired_table())

    # Then
    assert len(failures) == 1
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_rejects_all_non_nullable_column_adds_in_a_single_pass():
    # Given three NOT NULL column additions in a single dimensions tuple
    rule = NonNullableColumnAdd()
    dimensions = (
        ColumnStructureDimension(
            entries=(
                ColumnAdded(Column("a", Integer(), nullable=False)),
                ColumnAdded(Column("b", String(), nullable=False)),
                ColumnAdded(Column("c", Integer(), nullable=False)),
            )
        ),
    )

    # When
    failures = rule.evaluate(dimensions, _desired_table())

    # Then
    assert len(failures) == 3
    assert {f.rule_name for f in failures} == {"NonNullableColumnAdd"}
    messages = [f.message for f in failures]
    for col in ("a", "b", "c"):
        assert any(col in m for m in messages)


def test_allows_add_of_nullable_column():
    # Given a columns dimension containing a nullable column addition
    rule = NonNullableColumnAdd()
    dimensions = (ColumnStructureDimension(entries=(ColumnAdded(Column("age", Integer())),)),)

    assert rule.evaluate(dimensions, _desired_table()) == ()


def test_non_nullable_column_add_ignores_creation():
    # Given a TableMissing diff (table does not yet exist) with a NOT NULL column
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=False),),
    )

    # When
    result = validate_diff(TableMissing(desired=desired), desired)

    # Then
    assert result.failed is False


def test_non_nullable_column_add_passes_when_no_columns_dimension():
    # Given a dimensions tuple with no ColumnStructureDimension at all
    rule = NonNullableColumnAdd()

    assert rule.evaluate((), _desired_table()) == ()


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given a columns dimension with a nullability tightening on an existing column
    rule = NullabilityTighteningOnExistingColumn()
    dimensions = (ColumnStructureDimension(entries=(_tightening("order_id"),)),)

    # When
    failures = rule.evaluate(dimensions, _desired_table())

    # Then
    assert len(failures) == 1
    assert failures[0].rule_name == "NullabilityTighteningOnExistingColumn"
    assert "order_id" in failures[0].message


def test_rejects_all_nullability_tightenings_in_a_single_pass():
    # Given two nullability tightenings in a single dimensions tuple
    rule = NullabilityTighteningOnExistingColumn()
    dimensions = (ColumnStructureDimension(entries=(_tightening("a"), _tightening("b"))),)

    # When
    failures = rule.evaluate(dimensions, _desired_table())

    # Then
    assert len(failures) == 2
    messages = [f.message for f in failures]
    for col in ("a", "b"):
        assert any(col in m for m in messages)


def test_allows_loosening_an_existing_column_to_nullable():
    # Given a columns dimension with a nullability loosening (NOT NULL → nullable)
    rule = NullabilityTighteningOnExistingColumn()
    loosening = ColumnNullabilityChanged(
        column_name="id", change=Changed(desired=True, observed=False)
    )
    dimensions = (ColumnStructureDimension(entries=(loosening,)),)

    assert rule.evaluate(dimensions, _desired_table()) == ()


# ---- unsupported drift → ValidationFailure


def test_validate_diff_surfaces_type_drift_as_failure():
    # Given a drift whose only dimension is a ColumnStructureDimension with a type change
    diff = TableDrift(dimensions=(ColumnStructureDimension(entries=(_type_drift("id"),)),))

    # When
    result = validate_diff(diff, _desired_table())

    # Then
    assert result.failed is True
    assert len(result.failures) == 1
    assert "id" in result.failures[0].message


def test_validate_diff_surfaces_partitioning_change_as_failure():
    # Given a drift whose only dimension is a partitioning change
    diff = TableDrift(
        dimensions=(PartitioningDimension(change=Changed(desired=("ds",), observed=())),)
    )

    # When
    result = validate_diff(diff, _desired_table())

    # Then
    assert result.failed is True
    assert any("partitioning" in f.message.lower() for f in result.failures)


def test_validate_diff_collects_both_unsupported_drift_and_rule_failures():
    # Given a drift with a type change (unsupported) AND a NOT NULL add (rule violation)
    diff = TableDrift(
        dimensions=(
            ColumnStructureDimension(
                entries=(
                    _type_drift("id"),
                    ColumnAdded(Column("new_col", Integer(), nullable=False)),
                )
            ),
        )
    )

    # When
    result = validate_diff(diff, _desired_table())

    # Then
    assert result.failed is True
    assert len(result.failures) == 2


# ---- validate_diff


def test_validation_passes_when_no_rule_is_broken():
    # Given a drift containing only a nullable column addition (breaks no rules)
    diff = TableDrift(
        dimensions=(ColumnStructureDimension(entries=(ColumnAdded(Column("age", Integer())),)),)
    )

    # When
    result = validate_diff(diff, _desired_table())

    # Then
    assert result.failed is False
    assert result.failures == ()


def test_empty_drift_produces_no_failures():
    result = validate_diff(TableDrift(), _desired_table())

    assert result.failed is False


def test_missing_table_passes_validation():
    # Given a TableMissing diff (table does not yet exist)
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))

    # When
    result = validate_diff(TableMissing(desired=desired), desired)

    # Then
    assert result.failed is False


def test_validation_uses_the_default_rules_when_none_are_supplied():
    # Given a drift with a NOT NULL column addition and no explicit rules argument
    diff = TableDrift(
        dimensions=(
            ColumnStructureDimension(entries=(ColumnAdded(Column("x", Integer(), nullable=False)),)),
        )
    )

    # When
    result = validate_diff(diff, _desired_table())

    # Then
    assert result.failed is True


def test_validation_passes_when_empty_rule_set_is_supplied():
    # Given a drift that would break a rule, but no rules are supplied
    # ColumnAdded breaks no rule when rules=() — ColumnDataTypeChangeNotSupported is suppressed too
    diff = TableDrift(
        dimensions=(
            ColumnStructureDimension(entries=(ColumnAdded(Column("x", Integer(), nullable=False)),)),
        )
    )

    # When
    result = validate_diff(diff, _desired_table(), rules=())

    # Then
    assert result.failed is False


def test_validation_result_failed_property():
    # Given a result with one failure and a result with no failures
    failing = ValidationResult(failures=(ValidationFailure(rule_name="X", message="broken"),))
    passing = ValidationResult()

    # Then
    assert failing.failed is True
    assert passing.failed is False


def test_default_rules_cover_all_precondition_policies():
    # Given the DEFAULT_RULES constant
    rule_names = {type(rule).__name__ for rule in DEFAULT_RULES}

    assert rule_names == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
        "ColumnDataTypeChangeNotSupported",
        "PartitioningChangeNotSupported",
        "UnmanagedDimensionDrift",
    }


# ---- UnmanagedDimensionDrift


def test_unmanaged_dimension_drift_fails_when_unmanaged_dimension_has_drifted():
    # Given a table that only manages table tags, but column structure has drifted
    desired = _desired_table(
        managed_aspects=frozenset({TableAspect.TABLE_TAGS})
    )
    rule = UnmanagedDimensionDrift()
    dimensions = (
        ColumnStructureDimension(entries=(ColumnAdded(Column("extra", Integer())),)),
    )

    failures = rule.evaluate(dimensions, desired)

    # Then one failure names the unmanaged aspect
    assert len(failures) == 1
    assert failures[0].rule_name == "UnmanagedDimensionDrift"
    assert "column structure" in failures[0].message.lower()


def test_unmanaged_dimension_drift_produces_one_failure_per_drifted_unmanaged_dimension():
    # Given two unmanaged dimensions that have drifted
    desired = _desired_table(
        managed_aspects=frozenset({TableAspect.TABLE_TAGS})
    )
    rule = UnmanagedDimensionDrift()
    dimensions = (
        ColumnStructureDimension(entries=(ColumnAdded(Column("extra", Integer())),)),
        TableCommentDimension(change=Changed(desired="new", observed="old")),
    )

    failures = rule.evaluate(dimensions, desired)

    assert len(failures) == 2


def test_unmanaged_dimension_drift_passes_when_no_unmanaged_dimension_has_drifted():
    # Given a metadata-only table where only a managed dimension (table comment) has drifted
    desired = _desired_table(
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT})
    )
    rule = UnmanagedDimensionDrift()
    dimensions = (
        TableCommentDimension(change=Changed(desired="new", observed="old")),
    )

    failures = rule.evaluate(dimensions, desired)

    assert failures == ()


def test_unmanaged_dimension_drift_passes_when_all_aspects_managed():
    # Given a fully managed table with column structure drift
    desired = _desired_table(managed_aspects=ALL_ASPECTS)
    rule = UnmanagedDimensionDrift()
    dimensions = (
        ColumnStructureDimension(entries=(ColumnAdded(Column("extra", Integer())),)),
    )

    # Then the rule passes — structure is managed
    assert rule.evaluate(dimensions, desired) == ()


# ---- TableMissing with COLUMN_STRUCTURE unmanaged


def test_validate_diff_fails_table_missing_when_column_structure_unmanaged():
    # Given a metadata-only definition for a table that does not exist
    desired = _desired_table(
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT, TableAspect.TABLE_TAGS})
    )

    result = validate_diff(TableMissing(desired=desired), desired)

    assert result.failed is True
    assert any("does not exist" in f.message for f in result.failures)


def test_validate_diff_passes_table_missing_when_column_structure_managed():
    # Given a fully managed definition for a missing table
    desired = _desired_table(managed_aspects=ALL_ASPECTS)

    result = validate_diff(TableMissing(desired=desired), desired)

    assert result.failed is False


# ---- DEFAULT_RULES coverage


def test_default_rules_include_unmanaged_dimension_drift():
    rule_names = {type(rule).__name__ for rule in DEFAULT_RULES}

    assert "UnmanagedDimensionDrift" in rule_names
