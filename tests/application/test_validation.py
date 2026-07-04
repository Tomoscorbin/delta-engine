from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import (
    DEFAULT_RULES,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    ValidationResult,
    validate_diff,
)
from delta_engine.domain.model import Column, DesiredTable, Integer, Long, QualifiedName, String
from delta_engine.domain.plan.diff import (
    Changed,
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnsDimension,
    PartitioningDimension,
    TableDrift,
    TableMissing,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


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
        ColumnsDimension(entries=(ColumnAdded(Column("order_id", Integer(), nullable=False)),)),
    )

    # When
    failures = rule.evaluate(dimensions)

    # Then
    assert len(failures) == 1
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_rejects_all_non_nullable_column_adds_in_a_single_pass():
    # Given three NOT NULL column additions in a single dimensions tuple
    rule = NonNullableColumnAdd()
    dimensions = (
        ColumnsDimension(
            entries=(
                ColumnAdded(Column("a", Integer(), nullable=False)),
                ColumnAdded(Column("b", String(), nullable=False)),
                ColumnAdded(Column("c", Integer(), nullable=False)),
            )
        ),
    )

    # When
    failures = rule.evaluate(dimensions)

    # Then
    assert len(failures) == 3
    assert {f.rule_name for f in failures} == {"NonNullableColumnAdd"}
    messages = [f.message for f in failures]
    for col in ("a", "b", "c"):
        assert any(col in m for m in messages)


def test_allows_add_of_nullable_column():
    # Given a columns dimension containing a nullable column addition
    rule = NonNullableColumnAdd()
    dimensions = (ColumnsDimension(entries=(ColumnAdded(Column("age", Integer())),)),)

    assert rule.evaluate(dimensions) == ()


def test_non_nullable_column_add_ignores_creation():
    # Given a TableMissing diff (table does not yet exist) with a NOT NULL column
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=False),),
    )

    # When
    result = validate_diff(TableMissing(desired=desired))

    # Then
    assert result.failed is False


def test_non_nullable_column_add_passes_when_no_columns_dimension():
    # Given a dimensions tuple with no ColumnsDimension at all
    rule = NonNullableColumnAdd()

    assert rule.evaluate(()) == ()


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given a columns dimension with a nullability tightening on an existing column
    rule = NullabilityTighteningOnExistingColumn()
    dimensions = (ColumnsDimension(entries=(_tightening("order_id"),)),)

    # When
    failures = rule.evaluate(dimensions)

    # Then
    assert len(failures) == 1
    assert failures[0].rule_name == "NullabilityTighteningOnExistingColumn"
    assert "order_id" in failures[0].message


def test_rejects_all_nullability_tightenings_in_a_single_pass():
    # Given two nullability tightenings in a single dimensions tuple
    rule = NullabilityTighteningOnExistingColumn()
    dimensions = (ColumnsDimension(entries=(_tightening("a"), _tightening("b"))),)

    # When
    failures = rule.evaluate(dimensions)

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
    dimensions = (ColumnsDimension(entries=(loosening,)),)

    assert rule.evaluate(dimensions) == ()


# ---- unsupported drift → ValidationFailure


def test_validate_diff_surfaces_type_drift_as_failure():
    # Given a drift whose only dimension is a ColumnsDimension with a type change
    diff = TableDrift(dimensions=(ColumnsDimension(entries=(_type_drift("id"),)),))

    # When
    result = validate_diff(diff)

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
    result = validate_diff(diff)

    # Then
    assert result.failed is True
    assert any("partitioning" in f.message.lower() for f in result.failures)


def test_validate_diff_collects_both_unsupported_drift_and_rule_failures():
    # Given a drift with a type change (unsupported) AND a NOT NULL add (rule violation)
    diff = TableDrift(
        dimensions=(
            ColumnsDimension(
                entries=(
                    _type_drift("id"),
                    ColumnAdded(Column("new_col", Integer(), nullable=False)),
                )
            ),
        )
    )

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is True
    assert len(result.failures) == 2


# ---- validate_diff


def test_validation_passes_when_no_rule_is_broken():
    # Given a drift containing only a nullable column addition (breaks no rules)
    diff = TableDrift(
        dimensions=(ColumnsDimension(entries=(ColumnAdded(Column("age", Integer())),)),)
    )

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is False
    assert result.failures == ()


def test_empty_drift_produces_no_failures():
    result = validate_diff(TableDrift())

    assert result.failed is False


def test_missing_table_passes_validation():
    # Given a TableMissing diff (table does not yet exist)
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))

    # When
    result = validate_diff(TableMissing(desired=desired))

    # Then
    assert result.failed is False


def test_validation_uses_the_default_rules_when_none_are_supplied():
    # Given a drift with a NOT NULL column addition and no explicit rules argument
    diff = TableDrift(
        dimensions=(
            ColumnsDimension(entries=(ColumnAdded(Column("x", Integer(), nullable=False)),)),
        )
    )

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is True


def test_validation_passes_when_empty_rule_set_is_supplied():
    # Given a drift that would break a rule, but no rules are supplied
    # ColumnAdded breaks no rule when rules=() — ColumnDataTypeChangeNotSupported is suppressed too
    diff = TableDrift(
        dimensions=(
            ColumnsDimension(entries=(ColumnAdded(Column("x", Integer(), nullable=False)),)),
        )
    )

    # When
    result = validate_diff(diff, rules=())

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
    }
