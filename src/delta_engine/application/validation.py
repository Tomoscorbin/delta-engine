"""Validation rules for planned schema changes."""

from __future__ import annotations

from abc import ABC, abstractmethod

from delta_engine.application.plan import PlanContext
from delta_engine.application.results import ValidationFailure
from delta_engine.domain.model import ObservedTable
from delta_engine.domain.plan import AddColumn, SetColumnNullability


class Rule(ABC):
    """
    A safety constraint a plan must satisfy before it is executed.

    A rule reads the plan (and the table it alters) and reports the failures it
    finds -- an empty tuple means the plan satisfies it. Subclasses implement
    :meth:`check` top to bottom and build each failure with :meth:`_violation`,
    which attaches the rule's class name and the shared prefix.

    Rules run only against *alterations*: the validator skips them entirely when
    there is no observed table, so :meth:`check` always receives the existing
    table and never has to consider creation.
    """

    @abstractmethod
    def check(
        self, ctx: PlanContext, observed: ObservedTable
    ) -> tuple[ValidationFailure, ...]:
        """Return the failures this rule finds in the plan; empty means satisfied."""
        ...

    def _violation(self, reason: str) -> ValidationFailure:
        """Build a failure attributed to this rule, with the shared prefix."""
        return ValidationFailure(
            rule_name=type(self).__name__,
            message=f"Operation not allowed: {reason}",
        )


class NonNullableColumnAdd(Rule):
    """
    Disallow adding non-nullable columns to existing tables.

    Flags any plan that adds a NOT NULL column (it does not attempt to infer
    whether the table holds rows).
    """

    def check(
        self, ctx: PlanContext, observed: ObservedTable
    ) -> tuple[ValidationFailure, ...]:
        """Flag the first NOT NULL column the plan adds."""
        for action in ctx.plan.actions:
            if isinstance(action, AddColumn) and not action.column.nullable:
                return (self._violation(f"cannot add non-nullable column '{action.column.name}'"),)
        return ()


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

    def check(
        self, ctx: PlanContext, observed: ObservedTable
    ) -> tuple[ValidationFailure, ...]:
        """Flag the first column the plan tightens to NOT NULL."""
        for action in ctx.plan.actions:
            if isinstance(action, SetColumnNullability) and not action.nullable:
                return (
                    self._violation(
                        f"cannot tighten existing column '{action.column_name}' to NOT NULL."
                        " Keep it nullable, backfill any NULLs in a separate step,"
                        " then set NOT NULL"
                    ),
                )
        return ()


class UnsupportedColumnTypeChange(Rule):
    """
    Disallow changing the data type of an existing column.

    The differ matches columns by name and has no type-change action, so a
    changed type (e.g. ``Integer`` -> ``Long``) would otherwise produce no
    action at all and the schema would silently drift from the declared spec
    while the run reports success. Until type migrations are supported, surface
    the drift as a validation failure instead of letting it vanish.
    """

    def check(
        self, ctx: PlanContext, observed: ObservedTable
    ) -> tuple[ValidationFailure, ...]:
        """Flag the first common column whose desired type differs from observed."""
        observed_types = {column.name: column.data_type for column in observed.columns}
        for desired_column in ctx.desired.columns:
            observed_type = observed_types.get(desired_column.name)
            if observed_type is not None and observed_type != desired_column.data_type:
                return (
                    self._violation(
                        f"cannot change the type of existing column '{desired_column.name}'"
                        f" from {observed_type} to {desired_column.data_type}."
                        " Type migrations are not supported; recreate the table to change a"
                        " column's type"
                    ),
                )
        return ()


class DisallowPartitioningChange(Rule):
    """
    Disallow any plan that attempts to change partitioning.

    Partitioning can only occur during the creation of a table.
    """

    def check(
        self, ctx: PlanContext, observed: ObservedTable
    ) -> tuple[ValidationFailure, ...]:
        """Flag a difference between desired and observed partition columns."""
        if ctx.desired.partitioned_by == observed.partitioned_by:
            return ()
        return (
            self._violation(
                "partitioning changes are not supported."
                f" Current partition columns: {observed.partitioned_by}"
                f" - Requested partition columns: {ctx.desired.partitioned_by}."
                " Recreate the table with the desired partitioning"
            ),
        )


class PlanValidator:
    """Run a sequence of validation rules against a plan."""

    def __init__(self, rules: tuple[Rule, ...]) -> None:
        """Create a validator configured with an ordered set of rules."""
        self.rules = rules

    def validate(self, ctx: PlanContext) -> tuple[ValidationFailure, ...]:
        """
        Return one failure for each rule the plan breaks, in rule order.

        Rules constrain alterations, not creation: a table being created has no
        observed state to violate, so when it is absent no rule runs.
        """
        if ctx.observed is None:
            return ()
        failures: list[ValidationFailure] = []
        for rule in self.rules:
            failures.extend(rule.check(ctx, ctx.observed))
        return tuple(failures)


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    UnsupportedColumnTypeChange(),
    DisallowPartitioningChange(),
)
DEFAULT_VALIDATOR = PlanValidator(DEFAULT_RULES)
