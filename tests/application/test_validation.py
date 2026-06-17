from delta_engine.application.plan import PlanContext
from delta_engine.application.results import ValidationFailure
from delta_engine.application.validation import (
    DisallowPartitioningChange,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    PlanValidator,
    RuleResult,
    UnsupportedColumnTypeChange,
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

# ---- RuleResult


def test_satisfied_result_has_no_failures():
    # Given a rule that the plan satisfies
    result = RuleResult.satisfied()

    # Then it reports satisfied and carries no failures
    assert result.is_satisfied
    assert result.failures == ()


def test_violated_result_carries_its_failure():
    # Given a rule the plan violates
    failure = ValidationFailure(rule_name="SomeRule", message="nope")
    result = RuleResult.violated(failure)

    # Then it reports unsatisfied and carries the failure
    assert not result.is_satisfied
    assert result.failures == (failure,)


_QUALIFIED_NAME = QualifiedName("dev", "silver", "events")
_BASELINE_COLUMNS = (Column("id", Integer()),)


# ---- Helpers (real domain objects: no fakes for internal collaborators)


def _context(
    *,
    actions: tuple = (),
    observed_exists: bool = True,
    desired_columns: tuple[Column, ...] = _BASELINE_COLUMNS,
    observed_columns: tuple[Column, ...] = _BASELINE_COLUMNS,
    desired_partitions: tuple[str, ...] = (),
    observed_partitions: tuple[str, ...] = (),
) -> PlanContext:
    """Build a real PlanContext. `observed_exists=False` models a table creation."""
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=desired_columns,
        partitioned_by=desired_partitions,
    )
    observed = (
        ObservedTable(
            qualified_name=_QUALIFIED_NAME,
            columns=observed_columns,
            partitioned_by=observed_partitions,
        )
        if observed_exists
        else None
    )
    return PlanContext(
        desired=desired,
        observed=observed,
        plan=ActionPlan(target=_QUALIFIED_NAME, actions=actions),
    )


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column_on_existing_table():
    # Given an existing table and a plan that adds a NOT NULL column
    ctx = _context(actions=(AddColumn(column=Column("order_id", Integer(), nullable=False)),))

    # Then the rule flags the operation as invalid
    result = NonNullableColumnAdd().check(ctx)
    assert not result.is_satisfied
    assert "order_id" in result.failures[0].message


def test_allows_add_of_nullable_column_on_existing_table():
    # Given an existing table and a plan that adds a nullable column
    ctx = _context(actions=(AddColumn(column=Column("notes", String(), nullable=True)),))

    # Then the rule allows it
    assert NonNullableColumnAdd().check(ctx).is_satisfied


def test_allows_non_nullable_column_when_creating_new_table():
    # Given a new table creation (no observed table)
    ctx = _context(
        observed_exists=False,
        actions=(AddColumn(column=Column("id", Integer(), nullable=False)),),
    )

    # Then the rule does not block creation-time constraints
    assert NonNullableColumnAdd().check(ctx).is_satisfied


# ---- DisallowPartitioningChange


def test_rejects_partitioning_change_on_existing_table():
    # Given an existing table whose desired and observed partition specs differ
    ctx = _context(
        desired_columns=(Column("id", Integer()), Column("country", String())),
        observed_columns=(Column("id", Integer()), Column("ds", String())),
        observed_partitions=("ds",),
        desired_partitions=("country",),
    )

    # Then the rule flags the operation as invalid
    assert not DisallowPartitioningChange().check(ctx).is_satisfied


def test_allows_partitioning_on_new_table():
    # Given a new table creation (no observed table)
    ctx = _context(
        observed_exists=False,
        desired_columns=(Column("id", Integer()), Column("ds", String())),
        desired_partitions=("ds",),
    )

    # Then the rule allows it for table creation
    assert DisallowPartitioningChange().check(ctx).is_satisfied


def test_allows_when_partition_spec_unchanged_on_existing_table():
    # Given an existing table whose desired and observed partition specs are identical
    ctx = _context(
        desired_columns=(Column("id", Integer()), Column("ds", String())),
        observed_columns=(Column("id", Integer()), Column("ds", String())),
        observed_partitions=("ds",),
        desired_partitions=("ds",),
    )

    # Then there is no failure from this rule
    assert DisallowPartitioningChange().check(ctx).is_satisfied


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given an existing table and a plan that tightens a column to NOT NULL
    ctx = _context(actions=(SetColumnNullability(column_name="id", nullable=False),))

    # Then the rule flags it: tightening can fail at runtime if NULLs already exist
    result = NullabilityTighteningOnExistingColumn().check(ctx)
    assert not result.is_satisfied
    assert "id" in result.failures[0].message


def test_allows_loosening_an_existing_column_to_nullable():
    # Given an existing table and a plan that loosens a column to NULL (always safe)
    ctx = _context(actions=(SetColumnNullability(column_name="id", nullable=True),))

    # Then the rule allows it
    assert NullabilityTighteningOnExistingColumn().check(ctx).is_satisfied


def test_allows_tightening_when_creating_a_new_table():
    # Given a new table creation (no observed table)
    ctx = _context(
        observed_exists=False,
        actions=(SetColumnNullability(column_name="id", nullable=False),),
    )

    # Then creation-time constraints are not this rule's concern
    assert NullabilityTighteningOnExistingColumn().check(ctx).is_satisfied


# ---- UnsupportedColumnTypeChange


def test_rejects_changing_the_type_of_an_existing_column():
    # Given an existing column whose desired type differs from the observed type
    ctx = _context(
        desired_columns=(Column("id", Long()),),
        observed_columns=(Column("id", Integer()),),
    )

    # Then the drift is flagged rather than silently ignored
    result = UnsupportedColumnTypeChange().check(ctx)
    assert not result.is_satisfied
    assert "id" in result.failures[0].message


def test_allows_columns_whose_type_is_unchanged():
    # Given an existing table where every common column keeps its type
    ctx = _context(
        desired_columns=(Column("id", Integer()), Column("name", String())),
        observed_columns=(Column("id", Integer()), Column("name", String())),
    )

    # Then nothing is flagged
    assert UnsupportedColumnTypeChange().check(ctx).is_satisfied


def test_ignores_added_and_dropped_columns_for_type_change():
    # Given a column only in desired (add) and one only in observed (drop)
    ctx = _context(
        desired_columns=(Column("id", Integer()), Column("added", String())),
        observed_columns=(Column("id", Integer()), Column("dropped", String())),
    )

    # Then only common columns are compared; add/drop are not type changes
    assert UnsupportedColumnTypeChange().check(ctx).is_satisfied


def test_allows_type_specification_when_creating_a_new_table():
    # Given a new table creation (no observed table)
    ctx = _context(observed_exists=False, desired_columns=(Column("id", Long()),))

    # Then there is no prior type to conflict with
    assert UnsupportedColumnTypeChange().check(ctx).is_satisfied


# ---- PlanValidator


def test_validator_returns_empty_tuple_when_no_rules_fail():
    # Given a plan that violates no rules (new table, nullable column)
    ctx = _context(
        observed_exists=False,
        actions=(AddColumn(column=Column("x", String(), nullable=True)),),
    )
    validator = PlanValidator((NonNullableColumnAdd(), DisallowPartitioningChange()))

    # Then no failures are returned
    assert validator.validate(ctx) == ()


def test_validator_collects_a_failure_from_every_broken_rule():
    # Given an existing table whose plan breaks two rules at once
    ctx = _context(
        actions=(
            AddColumn(column=Column("order_id", Integer(), nullable=False)),  # NonNullableColumnAdd
            SetColumnNullability(column_name="id", nullable=False),  # tightening
        ),
    )
    validator = PlanValidator(
        (NonNullableColumnAdd(), NullabilityTighteningOnExistingColumn())
    )

    # Then both rules contribute a failure, named after the rule classes
    failures = validator.validate(ctx)
    assert {f.rule_name for f in failures} == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
    }


def test_empty_plan_produces_no_failures():
    # Given an existing table with an empty plan
    ctx = _context(actions=())
    validator = PlanValidator((NonNullableColumnAdd(), DisallowPartitioningChange()))

    # Then nothing fails because there is nothing to validate
    assert validator.validate(ctx) == ()
