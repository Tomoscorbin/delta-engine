"""Validated boundary from declared and observed state to executable action plans."""

from dataclasses import dataclass
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import DesiredTable, ObservedTable, TableAspect
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
    here: construction proves the plan targets the diff it was planned from
    and that the diff contains no unresolvable differences, so consumers never
    have to re-establish either invariant.
    """

    diff: TableDiff
    plan: ActionPlan

    def __post_init__(self) -> None:
        if self.plan.target != self.diff.target:
            raise ValueError("Accepted plan must target the diff it was planned from")
        if isinstance(self.diff, TableDrift) and self.diff.unresolvable:
            raise ValueError("Accepted plan cannot contain unresolvable differences")


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


@dataclass(frozen=True, slots=True)
class PlanningDeferred:
    """
    The table does not exist and this declaration cannot create it.

    Not a failure: a declaration whose scope does not manage table existence
    reconciles a portion of a table that must already exist, so the table's
    absence leaves it nothing to do — yet. The creation diff is retained so a
    report can still show what was declared absent. No action plan exists for
    this result variant; the sync converges vacuously and picks the table up
    once something else has created it.
    """

    diff: TableCreation


type PlanningResult = PlanningAccepted | PlanningRejected | PlanningDeferred


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
        case PlanningRejected() | PlanningDeferred() | None:
            return None
        case _ as unreachable:
            assert_never(unreachable)


def plan_changes(desired: DesiredTable, observed: ObservedTable | None) -> PlanningResult:
    """
    Plan the changes that reconcile ``observed`` with ``desired``; accept, reject, or defer.

    The one boundary from state to executable plan. It diffs the declaration
    against the observed table — ``None`` means confirmed absence, so the diff
    proposes creation — and validates the complete diff, foreign-key existence
    included, so policy sees exactly one stream with no side channels. An
    accepted result carries the validated :class:`ActionPlan`; a rejected
    result carries the validation failures and deliberately has no plan,
    making execution of unvalidated drift unrepresentable. This remains the
    only boundary that constructs an ``ActionPlan``, and every outcome retains
    the diff it was planned from. The plan carries the relation kind its
    actions lower against: the observed kind for drift, and the default
    ordinary kind for a creation.

    A proposed creation from a declaration whose scope does not manage table
    existence is deferred before validation ever sees it: the declaration
    cannot create the table, so a plan for the creation is unrepresentable
    rather than validated away, and the table's absence is not its failure.
    This is how a streaming table declared under ``"annotations"`` or
    ``"tags"`` waits for its pipeline to create it.
    """
    diff = diff_table(desired, observed)
    if isinstance(diff, TableCreation) and not desired.scope.manages(TableAspect.TABLE_EXISTENCE):
        return PlanningDeferred(diff=diff)

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
