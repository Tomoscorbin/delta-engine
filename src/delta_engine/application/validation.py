"""Validation rules for planned schema changes."""

from __future__ import annotations

from typing import ClassVar, Protocol

from delta_engine.application.results import ValidationFailure, ValidationResult
from delta_engine.domain.plan import (
    ActionPlan,
    AddColumn,
    CreateTable,
    SetColumnNullability,
    SetPrimaryKey,
    UnsupportedChange,
    UnsupportedChangeKind,
)


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
                    f"Operation not allowed: cannot add non-nullable column '{action.column.name}'"
                ),
            )
            for action in plan
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
            for action in plan
            if isinstance(action, SetColumnNullability) and not action.nullable
        )


class UnsupportedChangeRejected:
    """
    Disallow any change Delta Lake cannot apply in place.

    The differ emits an :class:`~delta_engine.domain.plan.UnsupportedChange`
    when it detects a column type change or a partitioning change. Neither is
    supported on an existing table, so this rule blocks any such action and
    surfaces the drift as a clear validation failure.
    """

    name: ClassVar[str] = "UnsupportedChangeRejected"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every UnsupportedChange action in the plan."""
        return tuple(
            ValidationFailure(rule_name=self.name, message=self._message(action))
            for action in plan
            if isinstance(action, UnsupportedChange)
        )

    def _message(self, action: UnsupportedChange) -> str:
        if action.kind is UnsupportedChangeKind.COLUMN_TYPE:
            return (
                "Operation not allowed: cannot change the type of existing column"
                f" '{action.subject_name}' from {action.from_repr} to {action.to_repr}."
                " Type migrations are not supported; recreate the table to change a column's type."
            )
        return (
            "Operation not allowed: partitioning changes are not supported."
            f" Current partition columns: {action.from_repr}"
            f" - Requested partition columns: {action.to_repr}."
            " Recreate the table with the desired partitioning."
        )


class PrimaryKeyColumnsNullable:
    """
    Disallow primary key constraints on nullable columns.

    Databricks rejects primary key constraints on nullable columns at execution
    time. This rule surfaces the violation at validation time — before any SQL
    is executed — with a clear message. Fires on both ``SetPrimaryKey`` actions
    (for existing tables) and ``CreateTable`` actions (for new tables with a
    declared primary key).
    """

    name: ClassVar[str] = "PrimaryKeyColumnsNullable"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        """Flag every nullable column declared as a primary key member."""
        failures = []
        for action in plan:
            if isinstance(action, SetPrimaryKey):
                for column in action.columns:
                    if column.nullable:
                        failures.append(
                            ValidationFailure(
                                rule_name=self.name,
                                message=(
                                    f"Operation not allowed: primary key column '{column.name}'"
                                    " must be NOT NULL. Set nullable=False on the column before"
                                    " declaring it as a primary key."
                                ),
                            )
                        )
            elif isinstance(action, CreateTable):
                if action.table.primary_key is not None:
                    primary_key_columns = set(action.table.primary_key.columns)
                    for column in action.table.columns:
                        if column.name in primary_key_columns and column.nullable:
                            failures.append(
                                ValidationFailure(
                                    rule_name=self.name,
                                    message=(
                                        f"Operation not allowed: primary key column '{column.name}'"
                                        " must be NOT NULL. Set nullable=False on the column before"
                                        " declaring it as a primary key."
                                    ),
                                )
                            )
        return tuple(failures)


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    UnsupportedChangeRejected(),
    PrimaryKeyColumnsNullable(),
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
    failures = tuple(failure for rule in rules for failure in rule.evaluate(plan))
    return ValidationResult(failures=failures)
