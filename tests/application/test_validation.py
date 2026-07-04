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
    Added,
    Changed,
    ColumnChanged,
    ColumnsDimension,
    PartitioningDimension,
    TableDrift,
    TableMissing,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _tightening(column_name: str = "id") -> ColumnChanged:
    return ColumnChanged(
        column_name=column_name, nullability=Changed(desired=False, observed=True)
    )


def _type_drift(column_name: str = "id") -> ColumnChanged:
    return ColumnChanged(
        column_name=column_name, data_type=Changed(desired=Long(), observed=Integer())
    )


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column():
    # Given a dimensions tuple containing a columns dimension with a NOT NULL addition
    rule = NonNullableColumnAdd()
    dimensions = (
        ColumnsDimension(entries=(Added(Column("order_id", Integer(), nullable=False)),)),
    )

    failures = rule.evaluate(dimensions)

    assert len(failures) == 1
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_rejects_all_non_nullable_column_adds_in_a_single_pass():
    rule = NonNullableColumnAdd()
    dimensions = (
        ColumnsDimension(
            entries=(
                Added(Column("a", Integer(), nullable=False)),
                Added(Column("b", String(), nullable=False)),
                Added(Column("c", Integer(), nullable=False)),
            )
        ),
    )

    failures = rule.evaluate(dimensions)

    assert len(failures) == 3
    assert {f.rule_name for f in failures} == {"NonNullableColumnAdd"}
    messages = [f.message for f in failures]
    for col in ("a", "b", "c"):
        assert any(col in m for m in messages)


def test_allows_add_of_nullable_column():
    rule = NonNullableColumnAdd()
    dimensions = (ColumnsDimension(entries=(Added(Column("age", Integer())),)),)

    assert rule.evaluate(dimensions) == ()


def test_non_nullable_column_add_ignores_creation():
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=False),),
    )

    result = validate_diff(TableMissing(desired=desired))

    assert result.failed is False


def test_non_nullable_column_add_passes_when_no_columns_dimension():
    # Given a dimensions tuple with no ColumnsDimension at all
    rule = NonNullableColumnAdd()

    assert rule.evaluate(()) == ()


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    rule = NullabilityTighteningOnExistingColumn()
    dimensions = (ColumnsDimension(entries=(_tightening("order_id"),)),)

    failures = rule.evaluate(dimensions)

    assert len(failures) == 1
    assert failures[0].rule_name == "NullabilityTighteningOnExistingColumn"
    assert "order_id" in failures[0].message


def test_rejects_all_nullability_tightenings_in_a_single_pass():
    rule = NullabilityTighteningOnExistingColumn()
    dimensions = (ColumnsDimension(entries=(_tightening("a"), _tightening("b"))),)

    failures = rule.evaluate(dimensions)

    assert len(failures) == 2
    messages = [f.message for f in failures]
    for col in ("a", "b"):
        assert any(col in m for m in messages)


def test_allows_loosening_an_existing_column_to_nullable():
    rule = NullabilityTighteningOnExistingColumn()
    loosening = ColumnChanged(
        column_name="id", nullability=Changed(desired=True, observed=False)
    )
    dimensions = (ColumnsDimension(entries=(loosening,)),)

    assert rule.evaluate(dimensions) == ()


# ---- unhandled facts → ValidationFailure


def test_validate_diff_surfaces_type_drift_as_failure():
    # Given a drift whose only dimension is a ColumnsDimension with a type change
    diff = TableDrift(
        dimensions=(ColumnsDimension(entries=(_type_drift("id"),)),)
    )

    result = validate_diff(diff)

    assert result.failed is True
    assert len(result.failures) == 1
    assert "id" in result.failures[0].message


def test_validate_diff_surfaces_partitioning_change_as_failure():
    diff = TableDrift(
        dimensions=(PartitioningDimension(change=Changed(desired=("ds",), observed=())),)
    )

    result = validate_diff(diff)

    assert result.failed is True
    assert any("partitioning" in f.message.lower() for f in result.failures)


def test_validate_diff_collects_both_unhandled_and_rule_failures():
    # Given a drift with a type change (unhandled) AND a NOT NULL add (rule)
    diff = TableDrift(
        dimensions=(
            ColumnsDimension(
                entries=(
                    _type_drift("id"),
                    Added(Column("new_col", Integer(), nullable=False)),
                )
            ),
        )
    )

    result = validate_diff(diff)

    assert result.failed is True
    assert len(result.failures) == 2


# ---- validate_diff


def test_validation_passes_when_no_rule_is_broken():
    diff = TableDrift(
        dimensions=(ColumnsDimension(entries=(Added(Column("age", Integer())),)),)
    )

    result = validate_diff(diff)

    assert result.failed is False
    assert result.failures == ()


def test_empty_drift_produces_no_failures():
    result = validate_diff(TableDrift())

    assert result.failed is False


def test_missing_table_passes_validation():
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))

    result = validate_diff(TableMissing(desired=desired))

    assert result.failed is False


def test_validation_uses_the_default_rules_when_none_are_supplied():
    diff = TableDrift(
        dimensions=(ColumnsDimension(entries=(Added(Column("x", Integer(), nullable=False)),)),)
    )

    result = validate_diff(diff)

    assert result.failed is True


def test_validation_passes_when_empty_rule_set_is_supplied():
    # Given a drift that would break a rule, but no rules are supplied
    # The unhandled type-drift still surfaces because it comes from dimensions, not rules
    diff = TableDrift(
        dimensions=(ColumnsDimension(entries=(Added(Column("x", Integer(), nullable=False)),)),)
    )

    result = validate_diff(diff, rules=())

    assert result.failed is False


def test_validation_result_failed_property():
    failing = ValidationResult(failures=(ValidationFailure(rule_name="X", message="broken"),))
    passing = ValidationResult()

    assert failing.failed is True
    assert passing.failed is False


def test_default_rules_cover_the_two_precondition_policies():
    rule_names = {type(rule).__name__ for rule in DEFAULT_RULES}

    assert rule_names == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
    }
