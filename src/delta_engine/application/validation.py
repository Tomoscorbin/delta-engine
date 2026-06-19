"""Validation rules for planned schema changes."""

from __future__ import annotations

from typing import ClassVar, Protocol

from delta_engine.application.results import ValidationFailure, ValidationResult
from delta_engine.domain.plan import ActionPlan, AddColumn, ColumnTypeChange, PartitioningChange, SetColumnNullability


class Rule(Protocol):
    """Interface for plan validation rules."""

    name: ClassVar[str]

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """
        Evaluate the rule against a planned change.

        Args:
            plan: The action plan to reach the desired state. A creation plan
                contains only a ``CreateTable`` action; a migration plan
                contains the specific change actions. Rules inspect the actions
                they care about and ignore the rest.

        Returns:
            A tuple of failures — one per violation found. Empty when the rule
            passes. All violations are returned in a single call so the caller
            reports the full set rather than requiring a fix-and-rerun cycle
            per failure.

        """
        ...


class NonNullableColumnAdd:
    """
    Disallow adding non-nullable columns to existing tables.

    The rule flags any plan that adds a NOT NULL column when the table
    already exists (it does not attempt to infer data emptiness).
    """

    name: ClassVar[str] = "NonNullableColumnAdd"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot add non-nullable"
                    f" column '{action.column.name}'"
                ),
            )
            for action in plan.actions
            if isinstance(action, AddColumn) and not action.column.nullable
        )


class NullabilityTighteningOnExistingColumn:
    """
    Disallow tightening an existing column to NOT NULL.

    Setting a previously-nullable column to NOT NULL fails at execution time if
    the column already holds NULLs, and the failure surfaces only after earlier
    actions have committed. The plan cannot know whether data is present, so --
    like :class:`NonNullableColumnAdd` -- the rule conservatively blocks the
    tightening and points to the safe path. Loosening to nullable is always safe
    and is not flagged.
    """

    name: ClassVar[str] = "NullabilityTighteningOnExistingColumn"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every action that tightens an existing column to NOT NULL."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot tighten existing column"
                    f" '{action.column_name}' to NOT NULL. Keep it nullable,"
                    " backfill any NULLs in a separate step, then set NOT NULL."
                ),
            )
            for action in plan.actions
            if isinstance(action, SetColumnNullability) and not action.nullable
        )


class UnsupportedColumnTypeChange:
    """
    Disallow changing the data type of an existing column.

    The differ emits a :class:`~delta_engine.domain.plan.ColumnTypeChange`
    action when it detects a type mismatch between desired and observed. Delta
    Lake does not support type migrations, so this rule blocks any such action
    and surfaces the drift as a clear validation failure.
    """

    name: ClassVar[str] = "UnsupportedColumnTypeChange"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every ColumnTypeChange action in the plan."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: cannot change the type of existing"
                    f" column '{action.column_name}' from {action.from_type} to"
                    f" {action.to_type}. Type migrations are not supported;"
                    " recreate the table to change a column's type."
                ),
            )
            for action in plan.actions
            if isinstance(action, ColumnTypeChange)
        )


class DisallowPartitioningChange:
    """
    Disallow any plan that attempts to change partitioning.

    The differ emits a :class:`~delta_engine.domain.plan.PartitioningChange`
    action when desired and observed partition specs differ. Partitioning can
    only be set during table creation, so this rule blocks any such action.
    """

    name: ClassVar[str] = "DisallowPartitioningChange"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag the plan if it contains a PartitioningChange action."""
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=(
                    "Operation not allowed: partitioning changes are not supported."
                    f" Current partition columns: {action.observed_partitioning}"
                    f" - Requested partition columns: {action.desired_partitioning}."
                    " Recreate the table with the desired partitioning."
                ),
            )
            for action in plan.actions
            if isinstance(action, PartitioningChange)
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    UnsupportedColumnTypeChange(),
    DisallowPartitioningChange(),
)


def validate_plan(
    plan: ActionPlan,
    rules: tuple[Rule, ...] = DEFAULT_RULES,
) -> ValidationResult:
    """
    Evaluate every rule against a planned change and return the verdict.

    A pure phase alongside :func:`~delta_engine.domain.plan.differ.compute_plan`:
    the same inputs always yield the same result. The caller reads
    ``ValidationResult.failed`` to gate execution; it does not assemble the verdict.

    Creation plans (containing only a ``CreateTable`` action) pass all rules
    automatically because none of the blocked action types appear in them.

    Args:
        plan: The action plan to reach the desired state.
        rules: The rules to apply, in evaluation order. Defaults to the full
            production set; override only to scope a check (e.g. in tests).

    Returns:
        A :class:`ValidationResult` carrying a failure from each broken rule.

    """
    failures = tuple(
        failure
        for rule in rules
        for failure in rule.evaluate(plan)
    )
    return ValidationResult(failures=failures)
