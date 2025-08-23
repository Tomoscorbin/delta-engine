from __future__ import annotations
from typing import ClassVar
from abc import ABC, abstractmethod

from tabula.application.plan.plan_context import PlanContext
from tabula.domain.plan.actions import AddColumn
from tabula.application.errors import ValidationError


class Rule(ABC):
    code: ClassVar[str]
    message: ClassVar[str]

    @abstractmethod
    def fails(self, ctx: PlanContext) -> bool:
        """
        True = rule is violated.
        False = rule is compliant.
        """
        raise NotImplementedError

    def __call__(self, ctx: PlanContext) -> bool:
        return self.fails(ctx)


class NoAddColumnOnNonEmptyTable(Rule):
    code: ClassVar[str] = "NO_ADD_ON_NON_EMPTY_TABLE"
    message: ClassVar[str] = "AddColumn actions are not allowed on non-empty tables."

    def fails(self, ctx: PlanContext) -> bool:
        if not ctx.subject.is_existing_and_non_empty:
            return False  # rule doesn't apply
        return any(isinstance(a, AddColumn) for a in ctx.plan.actions)


class PlanValidator:
    def __init__(self, rules: tuple[Rule, ...]) -> None:
        self.rules = rules

    def validate(self, ctx: PlanContext) -> None:
        for rule in self.rules:
            if rule.fails(ctx):
                raise ValidationError(rule.code, rule.message, ctx.target)


DEFAULT_RULES: tuple[Rule, ...] = (NoAddColumnOnNonEmptyTable(),)
DEFAULT_VALIDATOR = PlanValidator(DEFAULT_RULES)
