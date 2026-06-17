from delta_engine.application.plan import PlanContext
from delta_engine.application.validation import (
    DisallowPartitioningChange,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    PlanValidator,
)
from delta_engine.domain.model import Column, DesiredTable, Integer, ObservedTable, QualifiedName
from delta_engine.domain.plan.actions import ActionPlan, AddColumn, SetColumnNullability

# ---- Test fakes


class _FakeColumn:
    def __init__(self, name: str, nullable: bool) -> None:
        self.name = name
        self.nullable = nullable


class _FakePlan:
    def __init__(self, actions: tuple) -> None:
        self.actions = actions


class _FakeObserved:
    def __init__(self, partitioned_by: tuple[str, ...]) -> None:
        self.partitioned_by = partitioned_by


class _FakeDesired:
    def __init__(self, partitioned_by: tuple[str, ...]) -> None:
        self.partitioned_by = partitioned_by


class _FakeContext:
    def __init__(self, *, plan, observed, desired) -> None:
        self.plan = plan
        self.observed = observed
        self.desired = desired


# ---- Helpers


def _ctx_with_existing_table(
    actions: tuple, current_parts: tuple[str, ...] = (), desired_parts: tuple[str, ...] = ()
):
    plan = _FakePlan(actions)
    observed = _FakeObserved(partitioned_by=current_parts)
    desired = _FakeDesired(partitioned_by=desired_parts or current_parts)
    return _FakeContext(plan=plan, observed=observed, desired=desired)


def _ctx_for_create(actions: tuple, desired_parts: tuple[str, ...] = ()):
    plan = _FakePlan(actions)
    observed = None
    desired = _FakeDesired(partitioned_by=desired_parts)
    return _FakeContext(plan=plan, observed=observed, desired=desired)


# ---- Tests


def test_rejects_add_of_non_nullable_column_on_existing_table():
    # Given an existing table
    ctx = _ctx_with_existing_table(
        actions=(AddColumn(column=_FakeColumn(name="order_id", nullable=False)),)
    )
    rule = NonNullableColumnAdd()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule flags the operation as invalid
    assert failure is not None


def test_allows_add_of_nullable_column_on_existing_table():
    # Given an existing table
    ctx = _ctx_with_existing_table(
        actions=(AddColumn(column=_FakeColumn(name="notes", nullable=True)),)
    )
    rule = NonNullableColumnAdd()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule allows it
    assert failure is None


def test_allows_non_nullable_column_when_creating_new_table():
    # Given a new table creation (no observed table)
    ctx = _ctx_for_create(actions=(AddColumn(column=_FakeColumn(name="id", nullable=False)),))
    rule = NonNullableColumnAdd()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule does not block creation-time constraints
    assert failure is None


def test_rejects_partitioning_change_on_existing_table():
    # Given an existing table where desired and observed partition specs differ
    ctx = _ctx_with_existing_table(
        actions=(),
        current_parts=("ds",),
        desired_parts=("country",),
    )
    rule = DisallowPartitioningChange()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule flags the operation as invalid
    assert failure is not None


def test_allows_partitioning_on_new_table():
    # Given a new table creation (no observed table)
    ctx = _ctx_for_create(actions=(), desired_parts=("ds",))
    rule = DisallowPartitioningChange()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule allows it for table creation
    assert failure is None


def test_allows_when_partition_spec_unchanged_on_existing_table():
    # Given an existing table where desired and observed partition specs are identical
    ctx = _ctx_with_existing_table(
        actions=(AddColumn(column=_FakeColumn("x", True)),),
        current_parts=("ds",),
        desired_parts=("ds",),
    )
    rule = DisallowPartitioningChange()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then there is no failure from this rule
    assert failure is None


def test_validator_returns_empty_tuple_when_no_rules_fail():
    # Given a plan that violates no rules (new table, nullable column)
    ctx = _ctx_for_create(actions=(AddColumn(column=_FakeColumn("x", True)),))
    validator = PlanValidator((NonNullableColumnAdd(), DisallowPartitioningChange()))

    # When validating
    failures = validator.validate(ctx)

    # Then no failures are returned
    assert failures == ()


def test_empty_plan_produces_no_failures():
    # Given a plan with no actions
    empty_actions = tuple()
    ctx = _ctx_with_existing_table(actions=empty_actions)
    validator = PlanValidator((NonNullableColumnAdd(), DisallowPartitioningChange()))

    # When validating
    failures = validator.validate(ctx)

    # Then nothing fails because there is nothing to validate
    assert failures == ()


# ---- NullabilityTighteningOnExistingColumn (real domain objects)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "events")


def _context(*, observed: ObservedTable | None, actions: tuple) -> PlanContext:
    """Build a real PlanContext with the given plan actions for an existing/absent table."""
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))
    plan = ActionPlan(target=_QUALIFIED_NAME, actions=actions)
    return PlanContext(desired=desired, observed=observed, plan=plan)


def _existing_table() -> ObservedTable:
    return ObservedTable(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given an existing table and a plan that tightens a column to NOT NULL
    ctx = _context(
        observed=_existing_table(),
        actions=(SetColumnNullability(column_name="id", nullable=False),),
    )
    rule = NullabilityTighteningOnExistingColumn()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule flags it: tightening can fail at runtime if NULLs already exist
    assert failure is not None
    assert "id" in failure.message


def test_allows_loosening_an_existing_column_to_nullable():
    # Given an existing table and a plan that loosens a column to NULL (always safe)
    ctx = _context(
        observed=_existing_table(),
        actions=(SetColumnNullability(column_name="id", nullable=True),),
    )
    rule = NullabilityTighteningOnExistingColumn()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule allows it
    assert failure is None


def test_allows_tightening_when_creating_a_new_table():
    # Given a new table creation (no observed table)
    ctx = _context(
        observed=None,
        actions=(SetColumnNullability(column_name="id", nullable=False),),
    )
    rule = NullabilityTighteningOnExistingColumn()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then creation-time constraints are not this rule's concern
    assert failure is None
