"""Validated boundary from raw table diffs to executable action plans."""

from dataclasses import dataclass

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.plan import (
    ActionPlan,
    TableDiff,
)


@dataclass(frozen=True, slots=True)
class PlanningSucceeded:
    """Accepted diff and the validated executable plan constructed from it."""

    plan: ActionPlan


@dataclass(frozen=True, slots=True)
class PlanningFailed:
    """Rejected diff; no action plan exists for this result variant."""

    failures: tuple[ValidationFailure, ...]


type PlanningResult = PlanningSucceeded | PlanningFailed


def plan_diff(diff: TableDiff) -> PlanningResult:
    """
    Validate ``diff`` with the default policy and return an accepted or rejected result.

    This is the only boundary that constructs an :class:`ActionPlan` from a
    raw diff. A rejected result carries validation failures and deliberately
    has no plan, making execution of unvalidated drift unrepresentable.
    """
    validation = validate_diff(diff)
    if validation.failed:
        return PlanningFailed(failures=validation.failures)
    else:
        return PlanningSucceeded(plan=ActionPlan(diff.actions))
