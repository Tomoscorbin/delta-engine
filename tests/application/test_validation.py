from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import (
    DEFAULT_RULES,
    DisallowPartitioningChange,
    MissingTargetColumn,
    MissingTargetTable,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    UnenforceablePrimaryKeyChange,
    UnsupportedColumnTypeChange,
    ValidationResult,
    validate_plan,
)
from delta_engine.domain.model import (
    Column,
    DesiredTable,
    Integer,
    Long,
    QualifiedName,
    String,
    TableAspect,
)
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    ColumnTypeChange,
    CreateTable,
    PartitioningChange,
    SetColumnNullability,
    TargetColumnMissing,
    TargetTableMissing,
    UnenforceablePrimaryKey,
)


def _plan(*actions) -> ActionPlan:
    return ActionPlan(actions)


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column():
    # Given a plan adding a NOT NULL column to an existing table
    rule = NonNullableColumnAdd()

    # When evaluating
    failures = rule.evaluate(_plan(AddColumn(Column("order_id", Integer(), nullable=False))))

    # Then the violation is flagged
    assert len(failures) == 1
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_rejects_all_non_nullable_column_adds_in_a_single_pass():
    # Given a plan adding three NOT NULL columns at once
    rule = NonNullableColumnAdd()

    # When evaluating
    failures = rule.evaluate(
        _plan(
            AddColumn(Column("a", Integer(), nullable=False)),
            AddColumn(Column("b", String(), nullable=False)),
            AddColumn(Column("c", Integer(), nullable=False)),
        )
    )

    # Then all three violations are reported in one pass, not just the first
    assert len(failures) == 3
    assert {f.rule_name for f in failures} == {"NonNullableColumnAdd"}
    messages = [f.message for f in failures]
    for column_name in ("a", "b", "c"):
        assert any(column_name in message for message in messages)


def test_allows_add_of_nullable_column():
    # Given a plan adding a nullable column (always safe)
    rule = NonNullableColumnAdd()

    failures = rule.evaluate(_plan(AddColumn(Column("notes", String(), nullable=True))))
    assert failures == ()


def test_non_nullable_column_add_ignores_creation_plan():
    # Given a creation plan — AddColumn does not appear; CreateTable carries the columns
    # A creation plan will never contain AddColumn, so the rule always returns ()
    failures = NonNullableColumnAdd().evaluate(_plan())
    assert failures == ()


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given a plan that tightens a column to NOT NULL
    rule = NullabilityTighteningOnExistingColumn()

    failures = rule.evaluate(_plan(SetColumnNullability(column_name="id", nullable=False)))
    assert len(failures) == 1
    assert "id" in failures[0].message


def test_rejects_all_nullability_tightenings_in_a_single_pass():
    # Given a plan tightening two columns to NOT NULL at once
    rule = NullabilityTighteningOnExistingColumn()

    # When evaluating
    failures = rule.evaluate(
        _plan(
            SetColumnNullability(column_name="id", nullable=False),
            SetColumnNullability(column_name="name", nullable=False),
        )
    )

    # Then both violations are reported in one pass
    assert len(failures) == 2
    messages = [f.message for f in failures]
    for column_name in ("id", "name"):
        assert any(column_name in message for message in messages)


def test_allows_loosening_an_existing_column_to_nullable():
    # Given a plan that loosens a column to nullable (always safe)
    rule = NullabilityTighteningOnExistingColumn()

    failures = rule.evaluate(_plan(SetColumnNullability(column_name="id", nullable=True)))
    assert failures == ()


# ---- UnsupportedColumnTypeChange


def test_rejects_column_type_change():
    # Given a plan with a ColumnTypeChange action
    rule = UnsupportedColumnTypeChange()
    failures = rule.evaluate(
        _plan(ColumnTypeChange(column_name="id", from_type=Integer(), to_type=Long()))
    )
    # Then it is rejected with the column name in the message
    assert len(failures) == 1
    assert failures[0].rule_name == "UnsupportedColumnTypeChange"
    assert "id" in failures[0].message


def test_rejects_all_column_type_changes_in_a_single_pass():
    # Given a plan with two ColumnTypeChange actions
    rule = UnsupportedColumnTypeChange()
    failures = rule.evaluate(
        _plan(
            ColumnTypeChange(column_name="id", from_type=Integer(), to_type=Long()),
            ColumnTypeChange(column_name="score", from_type=String(), to_type=Integer()),
        )
    )
    # Then both are reported in one pass
    assert len(failures) == 2
    messages = [f.message for f in failures]
    for column_name in ("id", "score"):
        assert any(column_name in message for message in messages)


def test_allows_plan_with_no_column_type_change():
    # Given a plan with no ColumnTypeChange action
    rule = UnsupportedColumnTypeChange()
    failures = rule.evaluate(_plan(AddColumn(Column("new_col", String()))))
    assert failures == ()


# ---- DisallowPartitioningChange


def test_rejects_partitioning_change():
    # Given a plan with a PartitioningChange action
    rule = DisallowPartitioningChange()
    failures = rule.evaluate(
        _plan(PartitioningChange(desired_partitioning=("ds",), observed_partitioning=()))
    )
    # Then it is rejected
    assert len(failures) == 1
    assert failures[0].rule_name == "DisallowPartitioningChange"


def test_allows_plan_with_no_partitioning_change():
    # Given a plan with no PartitioningChange action
    rule = DisallowPartitioningChange()
    failures = rule.evaluate(_plan(AddColumn(Column("x", Integer()))))
    assert failures == ()


# ---- validate_plan


def test_validation_passes_when_no_rule_is_broken():
    # Given a plan that violates no rule
    rules = (NonNullableColumnAdd(), DisallowPartitioningChange())

    result = validate_plan(_plan(AddColumn(Column("x", String(), nullable=True))), rules=rules)

    assert not result.failed
    assert result.failures == ()


def test_validation_collects_a_failure_from_every_broken_rule():
    # Given a plan that breaks two rules at once
    rules = (NonNullableColumnAdd(), NullabilityTighteningOnExistingColumn())

    result = validate_plan(
        _plan(
            AddColumn(Column("order_id", Integer(), nullable=False)),
            SetColumnNullability(column_name="id", nullable=False),
        ),
        rules=rules,
    )

    assert result.failed
    assert {f.rule_name for f in result.failures} == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
    }


def test_empty_plan_produces_no_failures():
    # Given an empty plan
    rules = (NonNullableColumnAdd(), DisallowPartitioningChange())

    result = validate_plan(_plan(), rules=rules)

    assert not result.failed
    assert result.failures == ()


def test_validation_uses_the_default_rules_when_none_are_supplied():
    # Given a plan that the default NonNullableColumnAdd rule rejects
    result = validate_plan(_plan(AddColumn(Column("order_id", Integer(), nullable=False))))

    assert result.failed
    assert {f.rule_name for f in result.failures} == {"NonNullableColumnAdd"}


def test_validation_passes_when_empty_rule_set_is_supplied():
    # Given an empty rule set and a plan that the defaults WOULD reject
    result = validate_plan(
        _plan(AddColumn(Column("order_id", Integer(), nullable=False))),
        rules=(),
    )

    assert not result.failed
    assert result.failures == ()


def test_validation_result_failed_property_reflects_presence_of_failures():
    # Given a result with failures
    vf = ValidationFailure(rule_name="SomeRule", message="nope")

    # When checking .failed
    failed_result = ValidationResult(failures=(vf,))
    ok_result = ValidationResult()

    # Then it reports correctly
    assert failed_result.failed is True
    assert ok_result.failed is False


# A nullable primary key column is rejected when the DesiredTable is built (a
# desired-schema well-formedness invariant), not by a plan-validation rule — see
# tests/domain/model/test_table.py.


# ---- MissingTargetTable


def test_rejects_missing_table_that_this_definition_cannot_create():
    # Given a plan describing an absent table with column structure unmanaged
    rule = MissingTargetTable()

    failures = rule.evaluate(_plan(TargetTableMissing()))

    # Then the violation is flagged with a pointer to the resolution
    assert len(failures) == 1
    assert failures[0].rule_name == "MissingTargetTable"
    assert "does not exist" in failures[0].message


def test_missing_target_table_ignores_creation_plan():
    # Given an ordinary creation plan
    rule = MissingTargetTable()
    table = DesiredTable(
        qualified_name=QualifiedName("dev", "silver", "orders"),
        columns=(Column("id", Integer()),),
    )

    # Then no failure is raised
    assert rule.evaluate(_plan(CreateTable(table))) == ()


# ---- MissingTargetColumn


def test_rejects_metadata_targeting_a_column_missing_from_the_live_table():
    # Given a declared column absent live, targeted by a comment and tags
    rule = MissingTargetColumn()

    failures = rule.evaluate(
        _plan(
            TargetColumnMissing(
                column_name="email",
                reasons=(TableAspect.COLUMN_COMMENTS, TableAspect.COLUMN_TAGS),
            )
        )
    )

    # Then the failure names the column and every reason
    assert len(failures) == 1
    assert failures[0].rule_name == "MissingTargetColumn"
    assert "email" in failures[0].message
    assert "column comments" in failures[0].message
    assert "column tags" in failures[0].message


def test_rejects_every_missing_target_column_in_a_single_pass():
    # Given two broken target columns
    rule = MissingTargetColumn()

    failures = rule.evaluate(
        _plan(
            TargetColumnMissing(column_name="a", reasons=(TableAspect.PRIMARY_KEY,)),
            TargetColumnMissing(column_name="b", reasons=(TableAspect.FOREIGN_KEYS,)),
        )
    )

    # Then both violations are reported at once
    assert len(failures) == 2


# ---- UnenforceablePrimaryKeyChange


def test_rejects_primary_key_over_nullable_live_columns():
    # Given a planned PK whose columns are nullable in the live table
    rule = UnenforceablePrimaryKeyChange()

    failures = rule.evaluate(_plan(UnenforceablePrimaryKey(nullable_columns=("id", "region"))))

    # Then the failure names the nullable columns and the safe path
    assert len(failures) == 1
    assert failures[0].rule_name == "UnenforceablePrimaryKeyChange"
    assert "id" in failures[0].message
    assert "region" in failures[0].message


def test_new_rules_are_in_the_default_rule_set():
    # Given the production rule set
    rule_names = {type(rule).__name__ for rule in DEFAULT_RULES}

    # Then all three broken-target rules are active by default
    assert {
        "MissingTargetTable",
        "MissingTargetColumn",
        "UnenforceablePrimaryKeyChange",
    } <= rule_names
