"""Validated boundary from complete table diffs to executable action plans."""

from dataclasses import dataclass
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.plan import (
    ActionPlan,
    TableDiff,
    TableDrift,
    TableInSync,
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


def plan_diff(diff: TableDiff) -> PlanningResult:
    """
    Validate ``diff`` with the default policy and return an accepted or rejected result.

    This is the only boundary that constructs an :class:`ActionPlan` from a
    complete diff. A rejected result carries validation failures and deliberately
    has no plan, making execution of unvalidated drift unrepresentable.
    The plan carries the relation kind its actions lower against: the
    observed kind for an existing-table comparison, and the default ordinary
    kind for a creation — an absent table has no observed kind, and the engine
    only creates ordinary tables.
    """
    validation = validate_diff(diff)
    if validation.failed:
        return PlanningFailed(failures=validation.failures)
    match diff:
        case TableDrift() as drift:
            plan = ActionPlan(
                target=drift.target,
                actions=drift.actions,
                kind=drift.observed.kind,
            )
        case TableInSync() as in_sync:
            plan = ActionPlan(
                target=in_sync.target,
                actions=(),
                kind=in_sync.observed.kind,
            )
        case TableMissing() as missing:
            plan = ActionPlan(
                target=missing.target,
                actions=missing.actions,
            )
        case _ as unreachable:
            assert_never(unreachable)
    return PlanningSucceeded(plan=plan)
