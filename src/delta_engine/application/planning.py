"""Validated boundary from complete table diffs to executable action plans."""

from dataclasses import dataclass
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.plan import (
    ActionPlan,
    TableDiff,
    TableDrift,
    TableMissing,
)


@dataclass(frozen=True, slots=True)
class PlanningSucceeded:
    """
    The accepted diff and the validated executable plan constructed from it.

    The two facts are born together in :func:`plan_diff` and stay together
    here: construction proves the plan targets the diff it was judged from, so
    consumers never have to re-associate them.
    """

    diff: TableDiff
    plan: ActionPlan

    def __post_init__(self) -> None:
        if self.plan.target != self.diff.target:
            raise ValueError("Accepted plan must target the diff it was judged from")


@dataclass(frozen=True, slots=True)
class PlanningFailed:
    """
    The rejected diff and the failures that rejected it.

    No action plan exists for this result variant; the diff is retained so a
    report can still show what drifted.
    """

    diff: TableDiff
    failures: ListOrTuple[ValidationFailure]

    def __post_init__(self) -> None:
        object.__setattr__(self, "failures", tuple(self.failures))


type PlanningResult = PlanningSucceeded | PlanningFailed


def accepted_plan(planning: PlanningResult | None) -> ActionPlan | None:
    """
    Narrow a planning result to the plan it accepted, if any.

    The one place the union narrows to a plan. The report asks this of a
    result that may not exist — a failed read leaves no planning outcome — so
    the not-applicable case answers alongside the rejected one rather than at
    each caller.
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
    unvalidated drift unrepresentable. Both outcomes retain the diff they
    judged. The plan carries the relation kind its actions lower against: the
    observed kind for drift, and the default ordinary kind for a creation.
    """
    failures = validate_diff(diff)
    if failures:
        return PlanningFailed(diff=diff, failures=failures)
    match diff:
        case TableDrift() as drift:
            plan = ActionPlan(target=drift.target, actions=drift.actions, kind=drift.observed.kind)
        case TableMissing() as missing:
            plan = ActionPlan(target=missing.target, actions=missing.actions)
        case _ as unreachable:
            assert_never(unreachable)
    return PlanningSucceeded(diff=diff, plan=plan)
