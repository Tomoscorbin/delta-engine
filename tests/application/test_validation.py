from delta_engine.application.validation import (
    DisallowPartitioningChange,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    UnsupportedColumnTypeChange,
    validate_plan,
)
from delta_engine.domain.model import (
    Column,
    DesiredTable,
    Integer,
    Long,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.plan.actions import ActionPlan, AddColumn, SetColumnNullability

_QUALIFIED_NAME = QualifiedName("dev", "silver", "events")
_BASELINE_COLUMNS = (Column("id", Integer()),)


# ---- Helpers (real domain objects; no fakes for internal collaborators)


def _desired(
    *,
    columns: tuple[Column, ...] = _BASELINE_COLUMNS,
    partitioned_by: tuple[str, ...] = (),
) -> DesiredTable:
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME, columns=columns, partitioned_by=partitioned_by
    )


def _observed(
    *,
    columns: tuple[Column, ...] = _BASELINE_COLUMNS,
    partitioned_by: tuple[str, ...] = (),
) -> ObservedTable:
    return ObservedTable(
        qualified_name=_QUALIFIED_NAME, columns=columns, partitioned_by=partitioned_by
    )


def _plan(*actions) -> ActionPlan:
    return ActionPlan(actions)


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column_on_existing_table():
    # Given an existing table and a plan adding a NOT NULL column
    rule = NonNullableColumnAdd()

    # Then the rule flags the operation as invalid
    failure = rule.evaluate(
        _desired(), _observed(), _plan(AddColumn(Column("order_id", Integer(), nullable=False)))
    )
    assert failure is not None


def test_allows_add_of_nullable_column_on_existing_table():
    # Given an existing table and a plan adding a nullable column
    rule = NonNullableColumnAdd()

    # Then the rule allows it
    failure = rule.evaluate(
        _desired(), _observed(), _plan(AddColumn(Column("notes", String(), nullable=True)))
    )
    assert failure is None


def test_allows_non_nullable_column_when_creating_new_table():
    # Given a new table creation (no observed table)
    rule = NonNullableColumnAdd()

    # Then the rule does not block creation-time constraints
    failure = rule.evaluate(
        _desired(), None, _plan(AddColumn(Column("id", Integer(), nullable=False)))
    )
    assert failure is None


# ---- DisallowPartitioningChange


def test_rejects_partitioning_change_on_existing_table():
    # Given an existing table whose desired and observed partition specs differ
    rule = DisallowPartitioningChange()

    # Then the rule flags the operation as invalid
    failure = rule.evaluate(
        _desired(
            columns=(Column("id", Integer()), Column("country", String())),
            partitioned_by=("country",),
        ),
        _observed(
            columns=(Column("id", Integer()), Column("ds", String())),
            partitioned_by=("ds",),
        ),
        _plan(),
    )
    assert failure is not None


def test_allows_partitioning_on_new_table():
    # Given a new table creation (no observed table)
    rule = DisallowPartitioningChange()

    # Then the rule allows it for table creation
    failure = rule.evaluate(
        _desired(
            columns=(Column("id", Integer()), Column("ds", String())),
            partitioned_by=("ds",),
        ),
        None,
        _plan(),
    )
    assert failure is None


def test_allows_when_partition_spec_unchanged_on_existing_table():
    # Given an existing table whose desired and observed partition specs are identical
    rule = DisallowPartitioningChange()

    # Then there is no failure from this rule
    columns = (Column("id", Integer()), Column("ds", String()))
    failure = rule.evaluate(
        _desired(columns=columns, partitioned_by=("ds",)),
        _observed(columns=columns, partitioned_by=("ds",)),
        _plan(),
    )
    assert failure is None


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given an existing table and a plan that tightens a column to NOT NULL
    rule = NullabilityTighteningOnExistingColumn()

    # Then the rule flags it: tightening can fail at runtime if NULLs already exist
    failure = rule.evaluate(
        _desired(), _observed(), _plan(SetColumnNullability(column_name="id", nullable=False))
    )
    assert failure is not None
    assert "id" in failure.message


def test_allows_loosening_an_existing_column_to_nullable():
    # Given an existing table and a plan that loosens a column to NULL (always safe)
    rule = NullabilityTighteningOnExistingColumn()

    # Then the rule allows it
    failure = rule.evaluate(
        _desired(), _observed(), _plan(SetColumnNullability(column_name="id", nullable=True))
    )
    assert failure is None


def test_allows_tightening_when_creating_a_new_table():
    # Given a new table creation (no observed table)
    rule = NullabilityTighteningOnExistingColumn()

    # Then creation-time constraints are not this rule's concern
    failure = rule.evaluate(
        _desired(), None, _plan(SetColumnNullability(column_name="id", nullable=False))
    )
    assert failure is None


# ---- UnsupportedColumnTypeChange


def test_rejects_changing_the_type_of_an_existing_column():
    # Given an existing column whose desired type differs from the observed type
    rule = UnsupportedColumnTypeChange()

    # Then the drift is flagged rather than silently ignored
    failure = rule.evaluate(
        _desired(columns=(Column("id", Long()),)),
        _observed(columns=(Column("id", Integer()),)),
        _plan(),
    )
    assert failure is not None
    assert "id" in failure.message


def test_allows_columns_whose_type_is_unchanged():
    # Given an existing table where every common column keeps its type
    rule = UnsupportedColumnTypeChange()

    columns = (Column("id", Integer()), Column("name", String()))
    failure = rule.evaluate(_desired(columns=columns), _observed(columns=columns), _plan())
    assert failure is None


def test_ignores_added_and_dropped_columns_for_type_change():
    # Given a column only in desired (add) and one only in observed (drop)
    rule = UnsupportedColumnTypeChange()

    failure = rule.evaluate(
        _desired(columns=(Column("id", Integer()), Column("added", String()))),
        _observed(columns=(Column("id", Integer()), Column("dropped", String()))),
        _plan(),
    )
    assert failure is None


def test_allows_type_specification_when_creating_a_new_table():
    # Given a new table creation (no observed table)
    rule = UnsupportedColumnTypeChange()

    failure = rule.evaluate(_desired(columns=(Column("id", Long()),)), None, _plan())
    assert failure is None


# ---- validate_plan


def test_validation_passes_when_no_rule_is_broken():
    # Given a creation plan that violates no rule
    rules = (NonNullableColumnAdd(), DisallowPartitioningChange())

    # When validating
    result = validate_plan(
        _desired(), None, _plan(AddColumn(Column("x", String(), nullable=True))), rules=rules
    )

    # Then the verdict is a passing ValidationResult
    assert not result.failed
    assert result.failures == ()


def test_validation_collects_a_failure_from_every_broken_rule():
    # Given an existing table whose plan breaks two rules at once
    rules = (NonNullableColumnAdd(), NullabilityTighteningOnExistingColumn())

    # When validating
    result = validate_plan(
        _desired(),
        _observed(),
        _plan(
            AddColumn(Column("order_id", Integer(), nullable=False)),
            SetColumnNullability(column_name="id", nullable=False),
        ),
        rules=rules,
    )

    # Then the verdict fails, carrying a failure from each broken rule
    assert result.failed
    assert {f.rule_name for f in result.failures} == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
    }


def test_empty_plan_produces_no_failures():
    # Given an existing table with an empty plan
    rules = (NonNullableColumnAdd(), DisallowPartitioningChange())

    # When validating
    result = validate_plan(_desired(), _observed(), _plan(), rules=rules)

    # Then the verdict passes with no failures
    assert not result.failed
    assert result.failures == ()


def test_validation_uses_the_default_rules_when_none_are_supplied():
    # Given an existing table whose plan adds a non-nullable column
    # (no rules argument -> the engine's real default rule set applies)
    result = validate_plan(
        _desired(),
        _observed(),
        _plan(AddColumn(Column("order_id", Integer(), nullable=False))),
    )

    # Then the default NonNullableColumnAdd rule rejects it
    assert result.failed
    assert {f.rule_name for f in result.failures} == {"NonNullableColumnAdd"}
