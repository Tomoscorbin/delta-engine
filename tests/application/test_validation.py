from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import (
    DEFAULT_RULES,
    DisallowPartitioningChange,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    UnsupportedColumnTypeChange,
    ValidationResult,
    validate_diff,
)
from delta_engine.domain.model import Column, DesiredTable, Integer, Long, QualifiedName, String
from delta_engine.domain.plan.diff import (
    Added,
    Changed,
    ColumnChanged,
    TableDrift,
    TableMissing,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _type_drift(column_name: str = "id") -> ColumnChanged:
    return ColumnChanged(
        column_name=column_name, data_type=Changed(desired=Long(), observed=Integer())
    )


def _tightening(column_name: str = "id") -> ColumnChanged:
    return ColumnChanged(
        column_name=column_name, nullability=Changed(desired=False, observed=True)
    )


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column():
    # Given a drift adding a NOT NULL column to an existing table
    rule = NonNullableColumnAdd()

    # When evaluating
    failures = rule.evaluate(
        TableDrift(columns=(Added(Column("order_id", Integer(), nullable=False)),))
    )

    # Then the violation is flagged
    assert len(failures) == 1
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_rejects_all_non_nullable_column_adds_in_a_single_pass():
    # Given a drift adding three NOT NULL columns at once
    rule = NonNullableColumnAdd()

    # When evaluating
    failures = rule.evaluate(
        TableDrift(
            columns=(
                Added(Column("a", Integer(), nullable=False)),
                Added(Column("b", String(), nullable=False)),
                Added(Column("c", Integer(), nullable=False)),
            )
        )
    )

    # Then all three violations are reported in one pass, not just the first
    assert len(failures) == 3
    assert {failure.rule_name for failure in failures} == {"NonNullableColumnAdd"}
    messages = [failure.message for failure in failures]
    for column_name in ("a", "b", "c"):
        assert any(column_name in message for message in messages)


def test_allows_add_of_nullable_column():
    # Given a drift adding a nullable column
    rule = NonNullableColumnAdd()

    # Then no failure is raised
    assert rule.evaluate(TableDrift(columns=(Added(Column("age", Integer())),))) == ()


def test_non_nullable_column_add_ignores_creation():
    # Given a missing table whose declaration includes NOT NULL columns
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=False),),
    )

    # When validating the missing-table diff
    result = validate_diff(TableMissing(desired=desired))

    # Then creation is always safe — no rule sees it
    assert result.failed is False


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given a drift tightening an existing column to NOT NULL
    rule = NullabilityTighteningOnExistingColumn()

    failures = rule.evaluate(TableDrift(columns=(_tightening("order_id"),)))

    # Then the violation is flagged with the safe path
    assert len(failures) == 1
    assert failures[0].rule_name == "NullabilityTighteningOnExistingColumn"
    assert "order_id" in failures[0].message


def test_rejects_all_nullability_tightenings_in_a_single_pass():
    rule = NullabilityTighteningOnExistingColumn()

    failures = rule.evaluate(TableDrift(columns=(_tightening("a"), _tightening("b"))))

    assert len(failures) == 2
    messages = [failure.message for failure in failures]
    for column_name in ("a", "b"):
        assert any(column_name in message for message in messages)


def test_allows_loosening_an_existing_column_to_nullable():
    # Given a drift loosening a column to nullable — always safe
    rule = NullabilityTighteningOnExistingColumn()
    loosening = ColumnChanged(
        column_name="id", nullability=Changed(desired=True, observed=False)
    )

    assert rule.evaluate(TableDrift(columns=(loosening,))) == ()


# ---- UnsupportedColumnTypeChange


def test_rejects_column_type_change():
    # Given a drift where an existing column's type differs
    rule = UnsupportedColumnTypeChange()

    failures = rule.evaluate(TableDrift(columns=(_type_drift("id"),)))

    # Then the violation is flagged
    assert len(failures) == 1
    assert failures[0].rule_name == "UnsupportedColumnTypeChange"
    assert "id" in failures[0].message


def test_rejects_all_column_type_changes_in_a_single_pass():
    rule = UnsupportedColumnTypeChange()

    failures = rule.evaluate(TableDrift(columns=(_type_drift("a"), _type_drift("b"))))

    assert len(failures) == 2
    messages = [failure.message for failure in failures]
    for column_name in ("a", "b"):
        assert any(column_name in message for message in messages)


def test_allows_drift_with_no_column_type_change():
    # Given a changed column whose type is untouched
    rule = UnsupportedColumnTypeChange()

    assert rule.evaluate(TableDrift(columns=(_tightening(),))) == ()


# ---- DisallowPartitioningChange


def test_rejects_partitioning_change():
    # Given a drift where the partition specs differ
    rule = DisallowPartitioningChange()

    failures = rule.evaluate(
        TableDrift(partitioning=Changed(desired=("ds",), observed=()))
    )

    # Then the violation is flagged
    assert len(failures) == 1
    assert failures[0].rule_name == "DisallowPartitioningChange"


def test_allows_drift_with_no_partitioning_change():
    rule = DisallowPartitioningChange()

    assert rule.evaluate(TableDrift()) == ()


# ---- validate_diff


def test_validation_passes_when_no_rule_is_broken():
    # Given a benign drift
    result = validate_diff(TableDrift(columns=(Added(Column("age", Integer())),)))

    # Then no failures are reported
    assert result.failed is False
    assert result.failures == ()


def test_validation_collects_a_failure_from_every_broken_rule():
    # Given a drift breaking two rules at once
    result = validate_diff(
        TableDrift(
            columns=(_type_drift("id"),),
            partitioning=Changed(desired=("ds",), observed=()),
        )
    )

    # Then both failures are collected in one verdict
    assert result.failed is True
    assert {failure.rule_name for failure in result.failures} == {
        "UnsupportedColumnTypeChange",
        "DisallowPartitioningChange",
    }


def test_empty_drift_produces_no_failures():
    # Given a drift with no differences
    result = validate_diff(TableDrift())

    # Then validation passes
    assert result.failed is False


def test_missing_table_passes_validation():
    # Given a missing table — creation is always safe
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))

    result = validate_diff(TableMissing(desired=desired))

    assert result.failed is False


def test_validation_uses_the_default_rules_when_none_are_supplied():
    # Given a drift that breaks a default rule
    result = validate_diff(TableDrift(columns=(_type_drift(),)))

    # Then the default rule set catches it without rules being passed explicitly
    assert result.failed is True


def test_validation_passes_when_empty_rule_set_is_supplied():
    # Given a drift that would break a default rule, but no rules to apply
    result = validate_diff(TableDrift(columns=(_type_drift(),)), rules=())

    # Then nothing is evaluated and validation passes
    assert result.failed is False


def test_validation_result_failed_property_reflects_presence_of_failures():
    # Given results with and without failures
    failing = ValidationResult(
        failures=(ValidationFailure(rule_name="X", message="broken"),)
    )
    passing = ValidationResult()

    # Then failed mirrors the failures tuple
    assert failing.failed is True
    assert passing.failed is False


def test_default_rules_cover_the_four_safety_policies():
    # Given the production rule set
    rule_names = {type(rule).__name__ for rule in DEFAULT_RULES}

    # Then all four safety policies are active by default
    assert rule_names == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
        "UnsupportedColumnTypeChange",
        "DisallowPartitioningChange",
    }
