"""Validation rules for planned schema changes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from delta_engine.application.plan import PlanContext
from delta_engine.application.results import ValidationFailure
from delta_engine.domain.model import ObservedTable
from delta_engine.domain.plan import AddColumn, SetColumnNullability


@dataclass(frozen=True, slots=True)
class RuleResult:
    """
    The outcome of checking one rule against a plan.

    Build it through :meth:`satisfied` or :meth:`violated` rather than
    constructing it directly -- those name the two outcomes so call sites read
    plainly. ``is_satisfied`` answers the yes/no question; ``failures`` exposes
    the (zero or one) failures so a caller can aggregate them without testing
    for a sentinel.
    """

    failures: tuple[ValidationFailure, ...]

    @classmethod
    def satisfied(cls) -> RuleResult:
        """Build the result for a plan that satisfies the rule."""
        return cls(failures=())

    @classmethod
    def violated(cls, failure: ValidationFailure) -> RuleResult:
        """Build the result for a plan that violates the rule, described by ``failure``."""
        return cls(failures=(failure,))

    @property
    def is_satisfied(self) -> bool:
        """True when the plan satisfies the rule."""
        return not self.failures


class Rule(ABC):
    """
    A safety constraint a plan must satisfy before it is executed.

    Rules constrain *alterations* to an existing table, not table creation: a
    fresh table can be created with any shape, so a rule has nothing to check
    when there is no observed table. The base class encodes that precondition
    once and names each failure after the rule's class, so a concrete rule only
    has to answer one question -- :meth:`violated_by` -- and receives the
    observed table directly, already known to exist.
    """

    def check(self, ctx: PlanContext) -> RuleResult:
        """Return whether the plan satisfies this rule, with any failure."""
        if ctx.observed is None:
            return RuleResult.satisfied()
        reason = self.violated_by(ctx, ctx.observed)
        if reason is None:
            return RuleResult.satisfied()
        return RuleResult.violated(
            ValidationFailure(
                rule_name=type(self).__name__,
                message=f"Operation not allowed: {reason}",
            )
        )

    @abstractmethod
    def violated_by(self, ctx: PlanContext, observed: ObservedTable) -> str | None:
        """
        Return why the plan violates this rule, or ``None`` if it does not.

        Called only when the table already exists; ``observed`` is that current
        state, passed in so implementations need not re-check for creation. The
        returned text explains the violation; the base class prefixes it and
        attaches the rule's name.
        """
        ...


class NonNullableColumnAdd(Rule):
    """
    Disallow adding non-nullable columns to existing tables.

    Flags any plan that adds a NOT NULL column (it does not attempt to infer
    whether the table holds rows).
    """

    def violated_by(self, ctx: PlanContext, observed: ObservedTable) -> str | None:
        """Flag the first NOT NULL column the plan adds."""
        for action in ctx.plan.actions:
            if isinstance(action, AddColumn) and not action.column.nullable:
                return f"cannot add non-nullable column '{action.column.name}'"
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

    def violated_by(self, ctx: PlanContext, observed: ObservedTable) -> str | None:
        """Flag the first column the plan tightens to NOT NULL."""
        for action in ctx.plan.actions:
            if isinstance(action, SetColumnNullability) and not action.nullable:
                return (
                    f"cannot tighten existing column '{action.column_name}' to NOT NULL."
                    " Keep it nullable, backfill any NULLs in a separate step,"
                    " then set NOT NULL"
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

    def violated_by(self, ctx: PlanContext, observed: ObservedTable) -> str | None:
        """Flag the first common column whose desired type differs from observed."""
        observed_types = {column.name: column.data_type for column in observed.columns}
        for desired_column in ctx.desired.columns:
            observed_type = observed_types.get(desired_column.name)
            if observed_type is not None and observed_type != desired_column.data_type:
                return (
                    f"cannot change the type of existing column '{desired_column.name}'"
                    f" from {observed_type} to {desired_column.data_type}."
                    " Type migrations are not supported; recreate the table to change a"
                    " column's type"
                )
        return None


class DisallowPartitioningChange(Rule):
    """
    Disallow any plan that attempts to change partitioning.

    Partitioning can only occur during the creation of a table.
    """

    def violated_by(self, ctx: PlanContext, observed: ObservedTable) -> str | None:
        """Flag a difference between desired and observed partition columns."""
        if ctx.desired.partitioned_by == observed.partitioned_by:
            return None
        return (
            "partitioning changes are not supported."
            f" Current partition columns: {observed.partitioned_by}"
            f" - Requested partition columns: {ctx.desired.partitioned_by}."
            " Recreate the table with the desired partitioning"
        )


class PlanValidator:
    """Run a sequence of validation rules against a plan."""

    def __init__(self, rules: tuple[Rule, ...]) -> None:
        """Create a validator configured with an ordered set of rules."""
        self.rules = rules

    def validate(self, ctx: PlanContext) -> tuple[ValidationFailure, ...]:
        """Return one failure for each rule the plan breaks, in rule order."""
        failures: list[ValidationFailure] = []
        for rule in self.rules:
            failures.extend(rule.check(ctx).failures)
        return tuple(failures)


DEFAULT_RULES: tuple[Rule, ...] = (
    NonNullableColumnAdd(),
    NullabilityTighteningOnExistingColumn(),
    UnsupportedColumnTypeChange(),
    DisallowPartitioningChange(),
)
DEFAULT_VALIDATOR = PlanValidator(DEFAULT_RULES)
