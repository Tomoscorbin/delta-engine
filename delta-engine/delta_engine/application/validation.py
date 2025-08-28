"""Validation rules for planned schema changes."""

from __future__ import annotations

from abc import ABC, abstractmethod

from delta_engine.application.plan import PlanContext
from delta_engine.application.results import ValidationFailure
from delta_engine.domain.plan import AddColumn


class Rule(ABC):
    """Abstract interface for plan validation rules."""
    @abstractmethod
    def evaluate(self, ctx: PlanContext) -> ValidationFailure | None:
        """Evaluate the rule against a planning context.

        Args:
            ctx: The plan context to validate.

        Returns:
            A failure description if the rule is violated, otherwise ``None``.
        """
        ...


class NonNullableColumnAdd(Rule):           # Are classes and ABCs the best approach?
    """Disallow adding non-nullable columns to non-empty existing tables."""
    def evaluate(self, ctx: PlanContext) -> ValidationFailure | None:
        if ctx.observed is None:
            return None
        for action in ctx.plan.actions:
            if isinstance(action, AddColumn) and (not action.column.is_nullable):
                return ValidationFailure(
                    rule_name=self.__class__.__name__,
                    message=(
                        "Operation not allowed: cannot add non-nullable"
                        f" column '{action.column.name}'",
                    )
                )
        return None

class PlanValidator:
    """Run a sequence of validation rules against a plan."""

    def __init__(self, rules: tuple[Rule, ...]) -> None:
        self.rules = rules

    def validate(self, ctx: PlanContext) -> tuple[ValidationFailure, ...]:
        """Evaluate all rules and collect any failures.

        Args:
            ctx: The plan context being validated.

        Returns:
            A tuple of failures in rule evaluation order (empty if none).
        """
        failures: list[ValidationFailure] = ()
        for rule in self.rules:
            failure = rule.evaluate(ctx)
            if failure is not None:
                failures.append(failure)
        return tuple(failures)


DEFAULT_RULES: tuple[Rule, ...] = (NonNullableColumnAdd(),)
DEFAULT_VALIDATOR = PlanValidator(DEFAULT_RULES)
