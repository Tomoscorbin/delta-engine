"""Validation rules for planned schema changes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from tabula.application.errors import ValidationError
from tabula.application.plan.plan_context import PlanContext
from tabula.domain.plan.actions import AddColumn


class Rule(ABC):
    """Base class for validation rules."""

    code: ClassVar[str]
    message: ClassVar[str]

    @abstractmethod
    def fails(self, ctx: PlanContext) -> bool:
        """Return ``True`` if the rule is violated."""

    def __call__(self, ctx: PlanContext) -> bool:
        return self.fails(ctx)


class NoAddColumnOnNonEmptyTable(Rule):
    """Disallow adding columns to non-empty tables."""

    code: ClassVar[str] = "NO_ADD_ON_NON_EMPTY_TABLE"
    message: ClassVar[str] = "AddColumn actions are not allowed on non-empty tables."

    def fails(self, ctx: PlanContext) -> bool:
        """Check whether any AddColumn actions target a non-empty table."""

        if not ctx.subject.is_existing_and_non_empty:
            return False  # rule doesn't apply
        return any(isinstance(a, AddColumn) for a in ctx.plan.actions)


class PlanValidator:
    """Run a sequence of validation rules against a plan."""

    def __init__(self, rules: tuple[Rule, ...]) -> None:
        """Initialize the validator with rules."""

        self.rules = rules

    def validate(self, ctx: PlanContext) -> None:
        """Validate the plan context, raising ``ValidationError`` on failure."""

        for rule in self.rules:
            if rule.fails(ctx):
                raise ValidationError(rule.code, rule.message, ctx.target)


DEFAULT_RULES: tuple[Rule, ...] = (NoAddColumnOnNonEmptyTable(),)
DEFAULT_VALIDATOR = PlanValidator(DEFAULT_RULES)
