"""Validated boundary from complete table diffs to executable action plans."""

from dataclasses import dataclass, replace
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.plan import (
    ActionPlan,
    DropForeignKey,
    SetForeignKey,
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


def plan_diff(
    diff: TableDiff,
    relationship_actions: tuple[SetForeignKey | DropForeignKey, ...] = (),
) -> PlanningResult:
    """
    Validate ``diff`` plus the resolver's relationship actions; accept or reject.

    Relationship actions are merged into the drift before validation so the
    scope gate and the safety rules judge one complete stream — the PK-drop
    exemption must see this table's foreign-key drops, wherever they were
    planned. For a missing table the actions join the plan after the gates:
    safety rules never run on a creation. This remains the only boundary that
    constructs an :class:`ActionPlan`; a rejected result carries validation
    failures and deliberately has no plan, making execution of unvalidated
    drift unrepresentable. The plan carries the relation kind its actions
    lower against: the observed kind for drift, and the default ordinary kind
    for a creation.
    """
    match diff:
        case TableDrift() as drift:
            merged = replace(drift, actions=(*drift.actions, *relationship_actions))
            validation = validate_diff(merged)
            if validation.failed:
                return PlanningFailed(failures=validation.failures)
            plan = ActionPlan(
                target=merged.target,
                actions=merged.actions,
                kind=merged.observed.kind,
            )
        case TableMissing() as missing:
            validation = validate_diff(missing)
            if validation.failed:
                return PlanningFailed(failures=validation.failures)
            plan = ActionPlan(
                target=missing.target,
                actions=(*missing.actions, *relationship_actions),
            )
        case _ as unreachable:
            assert_never(unreachable)
    return PlanningSucceeded(plan=plan)
