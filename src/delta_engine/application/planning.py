"""Validated boundary from declared and observed state to executable action plans."""

from dataclasses import dataclass
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan import (
    ActionPlan,
    TableCreation,
    TableDiff,
    TableDrift,
    diff_table,
)


@dataclass(frozen=True, slots=True)
class PlanningAccepted:
    """
    The accepted diff and the validated executable plan constructed from it.

    The two facts are born together in :func:`plan_changes` and stay together
    here: construction proves the plan targets the diff it was planned from,
    so consumers never have to re-associate them.
    """

    diff: TableDiff
    plan: ActionPlan

    def __post_init__(self) -> None:
        if self.plan.target != self.diff.target:
            raise ValueError("Accepted plan must target the diff it was planned from")


@dataclass(frozen=True, slots=True)
class PlanningRejected:
    """
    The rejected diff and the failures that rejected it.

    No action plan exists for this result variant; the diff is retained so a
    report can still show what drifted.
    """

    diff: TableDiff
    failures: ListOrTuple[ValidationFailure]

    def __post_init__(self) -> None:
        object.__setattr__(self, "failures", tuple(self.failures))


type PlanningResult = PlanningAccepted | PlanningRejected


def accepted_plan(planning: PlanningResult | None) -> ActionPlan | None:
    """
    Narrow a planning result to the plan it accepted, if any.

    The one place the union narrows to a plan. The report asks this of a
    result that may not exist — a failed read leaves no planning outcome — so
    the not-applicable case answers alongside the rejected one rather than at
    each caller.
    """
    match planning:
        case PlanningAccepted(plan=plan):
            return plan
        case PlanningRejected() | None:
            return None
        case _ as unreachable:
            assert_never(unreachable)


def plan_changes(desired: DesiredTable, observed: ObservedTable | None) -> PlanningResult:
    """
    Plan the changes that reconcile ``observed`` with ``desired``; accept or reject.

    The one boundary from state to executable plan. It diffs the declaration
    against the observed table — ``None`` means confirmed absence, so the diff
    proposes creation — and validates the complete diff, foreign-key existence
    included, so policy sees exactly one stream with no side channels. An
    accepted result carries the validated :class:`ActionPlan`; a rejected
    result carries the validation failures and deliberately has no plan,
    making execution of unvalidated drift unrepresentable. This remains the
    only boundary that constructs an ``ActionPlan``, and both outcomes retain
    the diff they were planned from. The plan carries the relation kind its
    actions lower against: the observed kind for drift, and the default
    ordinary kind for a creation.
    """
    diff = diff_table(desired, observed)
    failures = validate_diff(diff)
    if failures:
        return PlanningRejected(diff=diff, failures=failures)

    match diff:
        case TableDrift() as drift:
            plan = ActionPlan(target=drift.target, actions=drift.actions, kind=drift.observed.kind)
        case TableCreation() as creation:
            plan = ActionPlan(target=creation.target, actions=creation.actions)
        case _ as unreachable:
            assert_never(unreachable)
    return PlanningAccepted(diff=diff, plan=plan)
