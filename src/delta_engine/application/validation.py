"""Validation rules for planned schema changes."""

from __future__ import annotations

from abc import ABC, abstractmethod

from delta_engine.application.results import ValidationFailure, ValidationResult
from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan import ActionPlan, AddColumn, SetColumnNullability


class Rule(ABC):
    """Abstract interface for plan validation rules."""

    @abstractmethod
    def evaluate(
        self, desired: DesiredTable, observed: ObservedTable | None, plan: ActionPlan
    ) -> tuple[ValidationFailure, ...]:
        """
        Evaluate the rule against a planned change.

        Args:
            desired: The user-authored target definition.
            observed: The current catalog state, or ``None`` when the table is
                being created.
            plan: The action plan to reach ``desired``.

        Returns:
            A tuple of failures — one per violation found. Empty when the rule
            passes. Returning all violations in one call lets the caller report
            the full set rather than requiring a fix-and-rerun cycle per failure.

        """
        ...


class NonNullableColumnAdd(Rule):
    """
    Disallow adding non-nullable columns to existing tables.

    The rule flags any plan that adds a NOT NULL column when the table
    already exists (it does not attempt to infer data emptiness).
    """

    def evaluate(
        self, desired: DesiredTable, observed: ObservedTable | None, plan: ActionPlan
    ) -> tuple[ValidationFailure, ...]:
        """Flag every NOT NULL column addition to an existing table."""
        if observed is None:
            return ()
        return tuple(
            ValidationFailure(
                rule_name=self.__class__.__name__,
                message=(
                    "Operation not allowed: cannot add non-nullable"
                    f" column '{action.column.name}'"
                ),
            )
            for action in plan.actions
            if isinstance(action, AddColumn) and not action.column.nullable
        )


class NullabilityTighteningOnExistingColumn(Rule):
    """
    Disallow tightening an existing column to NOT NULL.

    Setting a previously-nullable column to NOT NULL fails at execution time if
    the column already holds NULLs, and the failure surfaces only after earlier
    actions have committed. The plan cannot know whether data is present, so --
    like :class:`NonNullableColumnAdd` -- the rule conservatively blocks the
    tightening and points to the safe path. Loosening to nullable is always safe
    and is not flagged.
    """

    def evaluate(
        self, desired: DesiredTable, observed: ObservedTable | None, plan: ActionPlan
    ) -> tuple[ValidationFailure, ...]:
        """Flag every action that tightens an existing column to NOT NULL."""
        if observed is None:
            return ()
        return tuple(
            ValidationFailure(
                rule_name=self.__class__.__name__,
                message=(
                    "Operation not allowed: cannot tighten existing column"
                    f" '{action.column_name}' to NOT NULL. Keep it nullable,"
                    " backfill any NULLs in a separate step, then set NOT NULL."
                ),
            )
            for action in plan.actions
            if isinstance(action, SetColumnNullability) and not action.nullable
        )


class UnsupportedColumnTypeChange(Rule):
    """
    Disallow changing the data type of an existing column.

    The differ matches columns by name and has no type-change action, so a
    changed type (e.g. ``Integer`` -> ``Long``) would otherwise produce no
    action at all and the schema would silently drift from the declared spec
    while the run reports success. Until type migrations are supported, surface
    the drift as a validation failure instead of letting it vanish.
    """

    def evaluate(
        self, desired: DesiredTable, observed: ObservedTable | None, plan: ActionPlan
    ) -> tuple[ValidationFailure, ...]:
        """Flag every common column whose desired data type differs from observed."""
        if observed is None:
            return ()
        observed_types = {column.name: column.data_type for column in observed.columns}
        return tuple(
            ValidationFailure(
                rule_name=self.__class__.__name__,
                message=(
                    "Operation not allowed: cannot change the type of existing"
                    f" column '{desired_column.name}' from {observed_types[desired_column.name]} to"
                    f" {desired_column.data_type}. Type migrations are not supported;"
                    " recreate the table to change a column's type."
                ),
            )
            for desired_column in desired.columns
            if desired_column.name in observed_types
            and observed_types[desired_column.name] != desired_column.data_type
        )


class DisallowPartitioningChange(Rule):
    """
    Disallow any plan that attempts to change partitioning.

    Partitioning can only occur during the creation of a table.
    """

    def evaluate(
        self, desired: DesiredTable, observed: ObservedTable | None, plan: ActionPlan
    ) -> tuple[ValidationFailure, ...]:
        """Flag the plan if desired and observed partition columns differ."""
        if observed is None:
            return ()
        if desired.partitioned_by != observed.partitioned_by:
            return (
                ValidationFailure(
                    rule_name=self.__class__.__name__,
                    message=(
                        "Operation not allowed: partitioning changes are not supported."
                        f" Current partition columns: {observed.partitioned_by}"
                        f" - Requested partition columns: {desired.partitioned_by}."
                        " Recreate the table with the desired partitioning."
                    ),
                ),
            )
        return ()


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    UnsupportedColumnTypeChange(),
    DisallowPartitioningChange(),
)


def validate_plan(
    desired: DesiredTable,
    observed: ObservedTable | None,
    plan: ActionPlan,
    rules: tuple[Rule, ...] = DEFAULT_RULES,
) -> ValidationResult:
    """
    Evaluate every rule against a planned change and return the verdict.

    A pure phase alongside :func:`~delta_engine.domain.plan.differ.compute_plan`:
    the same inputs always yield the same result. The caller reads
    ``ValidationResult.failed`` to gate execution; it does not assemble the verdict.

    Args:
        desired: The user-authored target definition.
        observed: The current catalog state, or ``None`` when creating.
        plan: The action plan to reach ``desired``.
        rules: The rules to apply, in evaluation order. Defaults to the full
            production set; override only to scope a check (e.g. in tests).

    Returns:
        A :class:`ValidationResult` carrying a failure from each broken rule.

    """
    failures = tuple(
        failure
        for rule in rules
        for failure in rule.evaluate(desired, observed, plan)
    )
    return ValidationResult(failures=failures)
