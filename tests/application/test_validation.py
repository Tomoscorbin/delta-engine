from delta_engine.application.plan import PlanContext
from delta_engine.application.validation import (
    DisallowPartitioningChange,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    PlanValidator,
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

_QUALIFIED_NAME = QualifiedName("dev", "silver", "events")
_BASELINE_COLUMNS = (Column("id", Integer()),)


# ---- Helpers (real domain objects: no fakes for internal collaborators)


def _altering(
    *,
    actions: tuple = (),
    desired_columns: tuple[Column, ...] = _BASELINE_COLUMNS,
    observed_columns: tuple[Column, ...] = _BASELINE_COLUMNS,
    desired_partitions: tuple[str, ...] = (),
    observed_partitions: tuple[str, ...] = (),
) -> tuple[PlanContext, ObservedTable]:
    """Build a context + observed table for a plan that alters an existing table."""
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=observed_columns,
        partitioned_by=observed_partitions,
    )
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=desired_columns,
        partitioned_by=desired_partitions,
    )
    ctx = PlanContext(
        desired=desired,
        observed=observed,
        plan=ActionPlan(target=_QUALIFIED_NAME, actions=actions),
    )
    return ctx, observed


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column_on_existing_table():
    # Given an existing table and a plan that adds a NOT NULL column
    ctx, observed = _altering(
        actions=(AddColumn(column=Column("order_id", Integer(), nullable=False)),)
    )

    # Then the rule flags the operation as invalid
    failures = NonNullableColumnAdd().check(ctx, observed)
    assert len(failures) == 1
    assert "order_id" in failures[0].message


def test_allows_add_of_nullable_column_on_existing_table():
    # Given an existing table and a plan that adds a nullable column
    ctx, observed = _altering(
        actions=(AddColumn(column=Column("notes", String(), nullable=True)),)
    )

    # Then the rule allows it
    assert NonNullableColumnAdd().check(ctx, observed) == ()


# ---- DisallowPartitioningChange


def test_rejects_partitioning_change_on_existing_table():
    # Given an existing table whose desired and observed partition specs differ
    ctx, observed = _altering(
        desired_columns=(Column("id", Integer()), Column("country", String())),
        observed_columns=(Column("id", Integer()), Column("ds", String())),
        observed_partitions=("ds",),
        desired_partitions=("country",),
    )

    # Then the rule flags the operation as invalid
    assert DisallowPartitioningChange().check(ctx, observed) != ()


def test_allows_when_partition_spec_unchanged_on_existing_table():
    # Given an existing table whose desired and observed partition specs are identical
    ctx, observed = _altering(
        desired_columns=(Column("id", Integer()), Column("ds", String())),
        observed_columns=(Column("id", Integer()), Column("ds", String())),
        observed_partitions=("ds",),
        desired_partitions=("ds",),
    )

    # Then there is no failure from this rule
    assert DisallowPartitioningChange().check(ctx, observed) == ()


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given an existing table and a plan that tightens a column to NOT NULL
    ctx, observed = _altering(actions=(SetColumnNullability(column_name="id", nullable=False),))

    # Then the rule flags it: tightening can fail at runtime if NULLs already exist
    failures = NullabilityTighteningOnExistingColumn().check(ctx, observed)
    assert len(failures) == 1
    assert "id" in failures[0].message


def test_allows_loosening_an_existing_column_to_nullable():
    # Given an existing table and a plan that loosens a column to NULL (always safe)
    ctx, observed = _altering(actions=(SetColumnNullability(column_name="id", nullable=True),))

    # Then the rule allows it
    assert NullabilityTighteningOnExistingColumn().check(ctx, observed) == ()


# ---- UnsupportedColumnTypeChange


def test_rejects_changing_the_type_of_an_existing_column():
    # Given an existing column whose desired type differs from the observed type
    ctx, observed = _altering(
        desired_columns=(Column("id", Long()),),
        observed_columns=(Column("id", Integer()),),
    )

    # Then the drift is flagged rather than silently ignored
    failures = UnsupportedColumnTypeChange().check(ctx, observed)
    assert len(failures) == 1
    assert "id" in failures[0].message


def test_allows_columns_whose_type_is_unchanged():
    # Given an existing table where every common column keeps its type
    ctx, observed = _altering(
        desired_columns=(Column("id", Integer()), Column("name", String())),
        observed_columns=(Column("id", Integer()), Column("name", String())),
    )

    # Then nothing is flagged
    assert UnsupportedColumnTypeChange().check(ctx, observed) == ()


def test_ignores_added_and_dropped_columns_for_type_change():
    # Given a column only in desired (add) and one only in observed (drop)
    ctx, observed = _altering(
        desired_columns=(Column("id", Integer()), Column("added", String())),
        observed_columns=(Column("id", Integer()), Column("dropped", String())),
    )

    # Then only common columns are compared; add/drop are not type changes
    assert UnsupportedColumnTypeChange().check(ctx, observed) == ()


# ---- PlanValidator


def _creation_context(actions: tuple = ()) -> PlanContext:
    """Build a context for creating a fresh table (no observed state)."""
    desired = DesiredTable(qualified_name=_QUALIFIED_NAME, columns=_BASELINE_COLUMNS)
    return PlanContext(
        desired=desired,
        observed=None,
        plan=ActionPlan(target=_QUALIFIED_NAME, actions=actions),
    )


def test_validator_skips_all_rules_when_creating_a_new_table():
    # Given a creation plan that would break rules if it were an alteration
    ctx = _creation_context(
        actions=(AddColumn(column=Column("id", Integer(), nullable=False)),)
    )
    validator = PlanValidator((NonNullableColumnAdd(), DisallowPartitioningChange()))

    # Then nothing is validated: rules constrain alterations, not creation
    assert validator.validate(ctx) == ()


def test_validator_collects_a_failure_from_every_broken_rule():
    # Given an existing table whose plan breaks two rules at once
    ctx, _ = _altering(
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


def test_validator_returns_empty_tuple_when_no_rule_is_broken():
    # Given an existing table whose plan breaks no rule
    ctx, _ = _altering(actions=(AddColumn(column=Column("notes", String(), nullable=True)),))
    validator = PlanValidator((NonNullableColumnAdd(), DisallowPartitioningChange()))

    # Then no failures are returned
    assert validator.validate(ctx) == ()
