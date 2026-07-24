"""Validated boundary from raw table diffs to executable action plans."""

from dataclasses import dataclass, replace
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.features import required_features
from delta_engine.application.validation import validate_diff
from delta_engine.domain.plan import (
    ActionPlan,
    EnableTableFeature,
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


def plan_diff(diff: TableDiff) -> PlanningResult:
    """
    Validate ``diff`` with the default policy and return an accepted or rejected result.

    This is the only boundary that constructs an :class:`ActionPlan` from a
    raw diff. A rejected result carries validation failures and deliberately
    has no plan, making execution of unvalidated drift unrepresentable.
    The plan carries the relation kind its actions lower against: the
    observed kind for drift, and the default ordinary kind for a creation —
    an absent table has no observed kind, and the engine only creates
    ordinary tables.
    """
    prepared_diff = _add_required_feature_enablements(diff)
    validation = validate_diff(prepared_diff)
    if validation.failed:
        return PlanningFailed(failures=validation.failures)
    match prepared_diff:
        case TableDrift() as drift:
            plan = ActionPlan(
                target=drift.target,
                actions=drift.actions,
                kind=drift.observed.kind,
            )
        case TableMissing() as missing:
            plan = ActionPlan(
                target=missing.target,
                actions=missing.actions,
            )
        case _ as unreachable:
            assert_never(unreachable)
    return PlanningSucceeded(plan=plan)


def _add_required_feature_enablements(diff: TableDiff) -> TableDiff:
    """Add upgrades required by an existing table's desired column types."""
    match diff:
        case TableMissing():
            return diff
        case TableDrift() as drift:
            missing = required_features(drift.desired.columns) - drift.observed.supported_features
            enablements = tuple(
                EnableTableFeature(feature)
                for feature in sorted(missing, key=lambda item: item.value)
            )
            return replace(drift, actions=(*enablements, *drift.actions))
        case _ as unreachable:
            assert_never(unreachable)
