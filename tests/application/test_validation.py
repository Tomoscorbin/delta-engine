from delta_engine.application.validation import (
    DisallowPartitioningChange,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    UnsupportedColumnTypeChange,
    validate_plan,
)
from delta_engine.domain.model import Column, Integer, Long, String
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    ColumnTypeChange,
    PartitioningChange,
    SetColumnNullability,
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
    assert {"a", "b", "c"} == {f.message.split("'")[1] for f in failures}


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
    cols = (Column("id", Integer()), Column("name", String()))

    # When evaluating
    failures = rule.evaluate(
        _plan(
            SetColumnNullability(column_name="id", nullable=False),
            SetColumnNullability(column_name="name", nullable=False),
        )
    )

    # Then both violations are reported in one pass
    assert len(failures) == 2
    assert {"id", "name"} == {f.message.split("'")[1] for f in failures}


def test_allows_loosening_an_existing_column_to_nullable():
    # Given a plan that loosens a column to nullable (always safe)
    rule = NullabilityTighteningOnExistingColumn()

    failures = rule.evaluate(_plan(SetColumnNullability(column_name="id", nullable=True)))
    assert failures == ()


# ---- UnsupportedColumnTypeChange


def test_rejects_column_type_change_action():
    # Given a plan containing a ColumnTypeChange (emitted by the differ on type drift)
    rule = UnsupportedColumnTypeChange()

    failures = rule.evaluate(
        _plan(ColumnTypeChange(column_name="id", from_type=Integer(), to_type=Long()))
    )
    assert len(failures) == 1
    assert failures[0].rule_name == "UnsupportedColumnTypeChange"
    assert "id" in failures[0].message


def test_rejects_all_type_changes_in_a_single_pass():
    # Given a plan with two ColumnTypeChange actions
    rule = UnsupportedColumnTypeChange()

    failures = rule.evaluate(
        _plan(
            ColumnTypeChange(column_name="id", from_type=Integer(), to_type=Long()),
            ColumnTypeChange(column_name="score", from_type=String(), to_type=Integer()),
        )
    )
    assert len(failures) == 2
    assert {f.rule_name for f in failures} == {"UnsupportedColumnTypeChange"}
    assert {"id", "score"} == {f.message.split("'")[1] for f in failures}


def test_allows_plan_with_no_type_changes():
    # Given a plan with no ColumnTypeChange actions
    rule = UnsupportedColumnTypeChange()

    failures = rule.evaluate(_plan(AddColumn(Column("new_col", String()))))
    assert failures == ()


# ---- DisallowPartitioningChange


def test_rejects_partitioning_change_action():
    # Given a plan containing a PartitioningChange (emitted by the differ on partition drift)
    rule = DisallowPartitioningChange()

    failures = rule.evaluate(
        _plan(PartitioningChange(desired_partitioning=("ds",), observed_partitioning=()))
    )
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
