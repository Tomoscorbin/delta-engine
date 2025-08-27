"""Validation rules for planned schema changes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from delta_engine.application.plan import PlanContext
from delta_engine.domain.plan import AddColumn
from delta_engine.application.results import ValidationFailure


class Rule(ABC):
    name: ClassVar[str]

    @abstractmethod
    def evaluate(self, ctx: PlanContext) -> ValidationFailure | None: ...


@dataclass(frozen=True, slots=True)
class ForbidNonNullableAddOnNonEmptyTable(Rule):
    name = "forbid-required-add-on-non-empty"

    def evaluate(self, ctx: PlanContext) -> ValidationFailure | None:
        if ctx.observed is None or ctx.observed.is_empty:
            return None
        for action in ctx.plan.actions:
            if isinstance(action, AddColumn) and (not action.column.is_nullable):
                return ValidationFailure(
                    rule_name=self.name,
                    fully_qualified_name=str(ctx.qualified_name),
                    message=(
                        f"Cannot add non-nullable column '{action.column.name}' "
                        f"to non-empty table {ctx.qualified_name} without a default."
                    ),
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
