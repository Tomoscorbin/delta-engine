"""Validation rules for planned schema changes."""

from __future__ import annotations

import logging

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from delta_engine.application.plan import PlanContext
from delta_engine.domain.plan import AddColumn
from delta_engine.application.results import ValidationFailure


_LOGGER = logging.getLogger(__name__)

class Rule(ABC):
    @abstractmethod
    def evaluate(self, ctx: PlanContext) -> ValidationFailure | None: ...


@dataclass(frozen=True, slots=True)        #TODO: shouldnt be dataclass
class NonNullableColumnAdd(Rule):
    def evaluate(self, ctx: PlanContext) -> ValidationFailure | None:
        if ctx.observed is None:
            return None
        for action in ctx.plan.actions:
            if isinstance(action, AddColumn) and (not action.column.is_nullable):
                return ValidationFailure(
                    rule_name=self.__class__.__name__, 
                    message=f"Operation not allowed: add non-nullable column '{action.column.name}'",
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
            failure = rule.evaluate(ctx) 
            if failure is not None:
                failures.append(failure)
        return tuple(failures)


DEFAULT_RULES: tuple[Rule, ...] = (NonNullableColumnAdd(),)
DEFAULT_VALIDATOR = PlanValidator(DEFAULT_RULES)
