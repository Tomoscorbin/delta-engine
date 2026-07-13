"""Validated boundary from raw table diffs to executable action plans."""

from dataclasses import dataclass
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.plan import (
    ActionPlan,
    CreateTable,
    SetColumnTag,
    SetForeignKey,
    SetTableTag,
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

    def __post_init__(self) -> None:
        object.__setattr__(self, "failures", tuple(self.failures))
        if not self.failures:
            raise ValueError("PlanningFailed requires at least one validation failure")


type PlanningResult = PlanningSucceeded | PlanningFailed


def plan_diff(diff: TableDiff) -> PlanningResult:
    """
    Validate ``diff`` with the default policy and return an accepted or rejected result.

    This is the only boundary that constructs an :class:`ActionPlan` from a
    raw diff. A rejected result carries validation failures and deliberately
    has no plan, making execution of unvalidated drift unrepresentable.
    """
    validation = validate_diff(diff)
    if validation.failures:
        return PlanningFailed(failures=validation.failures)
    return PlanningSucceeded(plan=_build_plan(diff))


def _build_plan(diff: TableDiff) -> ActionPlan:
    """Construct a plan privately after the default validation policy accepts a diff."""
    match diff:
        case TableMissing() as missing:
            return _build_missing_table_plan(missing)
        case TableDrift() as drift:
            return _build_drift_plan(drift)
        case _ as unreachable:
            assert_never(unreachable)


def _build_missing_table_plan(missing: TableMissing) -> ActionPlan:
    """Build creation and follow-up actions for an accepted missing-table diff."""
    desired = missing.desired
    table_tag_actions = tuple(
        SetTableTag(name=name, value=value) for name, value in desired.tags.items()
    )
    column_tag_actions = tuple(
        SetColumnTag(column_name=column.name, name=name, value=value)
        for column in desired.columns
        for name, value in column.tags.items()
    )
    foreign_key_actions = tuple(
        SetForeignKey(constraint=constraint) for constraint in desired.foreign_keys
    )
    return ActionPlan(
        (
            CreateTable(desired),
            *table_tag_actions,
            *column_tag_actions,
            *foreign_key_actions,
        )
    )


def _build_drift_plan(drift: TableDrift) -> ActionPlan:
    """Order the domain-projected actions for an accepted drift diff."""
    return ActionPlan(drift.executable_actions)
