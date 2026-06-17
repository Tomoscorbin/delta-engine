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
    ) -> ValidationFailure | None:
        """
        Evaluate the rule against a planned change.

        Args:
            desired: The user-authored target definition.
            observed: The current catalog state, or ``None`` when the table is
                being created.
            plan: The action plan to reach ``desired``.

        Returns:
            A failure description if the rule is violated, otherwise ``None``.

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
    ) -> ValidationFailure | None:
        """Flag the plan if it adds a NOT NULL column to an existing table."""
        if observed is None:
            return None
        for action in plan.actions:
            if isinstance(action, AddColumn) and (not action.column.nullable):
                return ValidationFailure(
                    rule_name=self.__class__.__name__,
                    message=(
                        "Operation not allowed: cannot add non-nullable"
                        f" column '{action.column.name}'"
                    ),
                )
        return None


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
    ) -> ValidationFailure | None:
        """Flag the plan if it tightens any existing column to NOT NULL."""
        if observed is None:
            return None
        for action in plan.actions:
            if isinstance(action, SetColumnNullability) and (not action.nullable):
                return ValidationFailure(
                    rule_name=self.__class__.__name__,
                    message=(
                        "Operation not allowed: cannot tighten existing column"
                        f" '{action.column_name}' to NOT NULL. Keep it nullable,"
                        " backfill any NULLs in a separate step, then set NOT NULL."
                    ),
                )
        return None


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
    ) -> ValidationFailure | None:
        """Flag any common column whose desired data type differs from observed."""
        if observed is None:
            return None
        observed_types = {column.name: column.data_type for column in observed.columns}
        for desired_column in desired.columns:
            observed_type = observed_types.get(desired_column.name)
            if observed_type is not None and observed_type != desired_column.data_type:
                return ValidationFailure(
                    rule_name=self.__class__.__name__,
                    message=(
                        "Operation not allowed: cannot change the type of existing"
                        f" column '{desired_column.name}' from {observed_type} to"
                        f" {desired_column.data_type}. Type migrations are not supported;"
                        " recreate the table to change a column's type."
                    ),
                )
        return None


class DisallowPartitioningChange(Rule):
    """
    Disallow any plan that attempts to change partitioning.

    Partitioning can only occur during the creation of a table.
    """

    def evaluate(
        self, desired: DesiredTable, observed: ObservedTable | None, plan: ActionPlan
    ) -> ValidationFailure | None:
        """Flag the plan if desired and observed partition columns differ."""
        if observed is None:
            return None
        if desired.partitioned_by != observed.partitioned_by:
            return ValidationFailure(
                rule_name=self.__class__.__name__,
                message=(
                    "Operation not allowed: partitioning changes are not supported."
                    f" Current partition columns: {observed.partitioned_by}"
                    f" - Requested partition columns: {desired.partitioned_by}."
                    " Recreate the table with the desired partitioning."
                ),
            )
        return None


class PlanValidator:
    """Run a sequence of validation rules against a planned change."""

    def __init__(self, rules: tuple[Rule, ...]) -> None:
        """Create a validator configured with an ordered set of rules."""
        self.rules = rules

    def validate(
        self, desired: DesiredTable, observed: ObservedTable | None, plan: ActionPlan
    ) -> ValidationResult:
        """
        Evaluate all rules and return the aggregate verdict.

        Args:
            desired: The user-authored target definition.
            observed: The current catalog state, or ``None`` when creating.
            plan: The action plan to reach ``desired``.

        Returns:
            A :class:`ValidationResult` whose ``failed`` gates execution. The
            caller reads the verdict; it does not assemble it.

        """
        failures: list[ValidationFailure] = []
        for rule in self.rules:
            failure = rule.evaluate(desired, observed, plan)
            if failure is not None:
                failures.append(failure)
        return ValidationResult(failures=tuple(failures))


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    UnsupportedColumnTypeChange(),
    DisallowPartitioningChange(),
)
DEFAULT_VALIDATOR = PlanValidator(DEFAULT_RULES)
