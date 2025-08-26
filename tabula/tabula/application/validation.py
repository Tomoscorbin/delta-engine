"""Validation rules for planned schema changes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from tabula.application.plan import PlanContext
from tabula.domain.plan import AddColumn


@dataclass(frozen=True, slots=True)
class ValidationFailure:
    """Single validation failure for a plan."""

    rule: str
    fully_qualified_name: str
    message: str

class Rule(ABC):
    name: ClassVar[str]

    @abstractmethod
    def evaluate(self, ctx: PlanContext) -> ValidationFailure | None: ...



class ForbidNonNullableAddOnNonEmptyTable(Rule):
    name = "forbid-required-add-on-non-empty"
    def evaluate(self, ctx: PlanContext) -> ValidationFailure | None:
        if ctx.observed is None or not ctx.observed.is_empty:
            return None
        for a in ctx.plan.actions:
            if isinstance(a, AddColumn) and (not a.column.is_nullable):
                return ValidationFailure(
                    rule=self.name,
                    message=(
                        f"Cannot add column '{a.column.name}' without default"
                        f" to non-empty table {ctx.qualified_name}."
                    ),
                    table=str(ctx.qualified_name),
                )
        return None

class PlanValidator:
    """Run a sequence of validation rules against a plan."""

    def __init__(self, rules: tuple[Rule, ...]) -> None:
        """Initialize the validator with rules."""
        self.rules = rules

    def validate(self, ctx: PlanContext) -> tuple[ValidationFailure, ...]:
        failures: list[ValidationFailure] = []
        for rule in self.rules:
            issue = rule.evaluate(ctx)
            if issue is not None:
                failures.append(issue)
        return tuple(failures)


DEFAULT_RULES: tuple[Rule, ...] = (ForbidNonNullableAddOnNonEmptyTable(),)
DEFAULT_VALIDATOR = PlanValidator(DEFAULT_RULES)
