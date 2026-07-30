"""Validated boundary from complete table diffs to executable action plans."""

from dataclasses import dataclass
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.plan import (
    ActionPlan,
    TableDiff,
    TableDrift,
    TableMissing,
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


def accepted_plan(planning: PlanningResult | None) -> ActionPlan | None:
    """
    Narrow a planning result to the plan it accepted, if any.

    The one place the union narrows to a plan. Every phase downstream of
    planning asks this question — the engine's run while the phases are still
    mutating, its frozen report afterwards — and both ask it of a result that
    may not exist yet, so the not-yet-planned case answers alongside the
    rejected one rather than at each caller.
    """
    match planning:
        case PlanningSucceeded(plan=plan):
            return plan
        case PlanningFailed() | None:
            return None
        case _ as unreachable:
            assert_never(unreachable)


def plan_diff(diff: TableDiff) -> PlanningResult:
    """
    Validate ``diff``; accept or reject.

    The diff is complete — foreign-key existence included — so policy judges
    exactly one stream with no side channels. This remains the only boundary
    that constructs an :class:`ActionPlan`; a rejected result carries
    validation failures and deliberately has no plan, making execution of
    unvalidated drift unrepresentable. The plan carries the relation kind its
    actions lower against: the observed kind for drift, and the default
    ordinary kind for a creation.
    """
    failures = validate_diff(diff)
    if failures:
        return PlanningFailed(failures=failures)
    match diff:
        case TableDrift() as drift:
            plan = ActionPlan(target=drift.target, actions=drift.actions, kind=drift.observed.kind)
        case TableMissing() as missing:
            plan = ActionPlan(target=missing.target, actions=missing.actions)
        case _ as unreachable:
            assert_never(unreachable)
    return PlanningSucceeded(plan=plan)
