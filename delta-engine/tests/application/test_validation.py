from delta_engine.application.validation import (
    DisallowPartitioningChange,
    NonNullableColumnAdd,
    PlanValidator,
)
from delta_engine.domain.plan.actions import AddColumn, PartitionBy

# ---- Test fakes


class _FakeColumn:
    def __init__(self, name: str, is_nullable: bool) -> None:
        self.name = name
        self.is_nullable = is_nullable


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
        actions=(AddColumn(column=_FakeColumn(name="order_id", is_nullable=False)),)
    )
    rule = NonNullableColumnAdd()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule flags the operation as invalid
    assert failure is not None


def test_allows_add_of_nullable_column_on_existing_table():
    # Given an existing table
    ctx = _ctx_with_existing_table(
        actions=(AddColumn(column=_FakeColumn(name="notes", is_nullable=True)),)
    )
    rule = NonNullableColumnAdd()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule allows it
    assert failure is None


def test_allows_non_nullable_column_when_creating_new_table():
    # Given a new table creation (no observed table)
    ctx = _ctx_for_create(actions=(AddColumn(column=_FakeColumn(name="id", is_nullable=False)),))
    rule = NonNullableColumnAdd()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule does not block creation-time constraints
    assert failure is None


def test_rejects_partitioning_change_on_existing_table():
    # Given an existing table with current partitioning
    current_parts = ("ds",)
    desired_parts = ("country",)
    ctx = _ctx_with_existing_table(
        actions=(PartitionBy(column_names=desired_parts),),
        current_parts=current_parts,
        desired_parts=desired_parts,
    )
    rule = DisallowPartitioningChange()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule flags the operation as invalid
    assert failure is not None


def test_allows_partitioning_statement_during_create():
    # Given a new table creation with desired partitioning
    ctx = _ctx_for_create(actions=(PartitionBy(column_names=("ds",)),), desired_parts=("ds",))
    rule = DisallowPartitioningChange()

    # When evaluating the plan
    failure = rule.evaluate(ctx)

    # Then the rule allows it for table creation
    assert failure is None


def test_ignores_plans_that_do_not_touch_partitioning_on_existing_table():
    # Given an existing table and a plan with no partitioning action
    ctx = _ctx_with_existing_table(actions=(AddColumn(column=_FakeColumn("x", True)),))
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
