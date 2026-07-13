"""Validated boundary from raw table diffs to executable action plans."""

from dataclasses import dataclass, replace
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.model import ForeignKeyConstraint, QualifiedName
from delta_engine.domain.plan import (
    Action,
    ActionPlan,
    CreateTable,
    DropForeignKey,
    DropPrimaryKey,
    RenameColumn,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
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
            desired = missing.desired
            tag_actions = tuple(
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
                    *tag_actions,
                    *column_tag_actions,
                    *foreign_key_actions,
                )
            )
        case TableDrift() as drift:
            return _build_drift_plan(drift)
        case _ as unreachable:
            assert_never(unreachable)


def _build_drift_plan(drift: TableDrift) -> ActionPlan:
    """Order accepted actions while accounting for constraints handled by a rename."""
    renamed_sources = frozenset(
        action.old_name for action in drift.changes if isinstance(action, RenameColumn)
    )
    renamed_primary_keys = frozenset(
        change.primary_key
        for change in drift.changes
        if isinstance(change, DropPrimaryKey)
        and not renamed_sources.isdisjoint(change.primary_key.columns)
    )
    actions: list[Action] = []
    for change in drift.changes:
        # The default rules reject every BlockingChange. Reaching this branch
        # therefore proves that each retained diff member is an executable action.
        if not isinstance(change, Action):
            continue
        if isinstance(change, DropPrimaryKey) and change.primary_key in renamed_primary_keys:
            continue
        if isinstance(change, DropForeignKey) and _foreign_key_uses_renamed_column(
            change.constraint,
            renamed_sources=renamed_sources,
            table_name=drift.desired.qualified_name,
        ):
            continue
        if (
            isinstance(change, SetPrimaryKey)
            and change.replaced_primary_key in renamed_primary_keys
        ):
            # The raw diff correlates replacements. RENAME COLUMN performs this
            # drop atomically, so the executable plan retains only an ordinary set.
            change = replace(change, replaced_primary_key=None)
        actions.append(change)
    return ActionPlan(tuple(actions))


def _foreign_key_uses_renamed_column(
    constraint: ForeignKeyConstraint,
    *,
    renamed_sources: frozenset[str],
    table_name: QualifiedName,
) -> bool:
    """Whether a local or self-referenced FK column is renamed by this table."""
    local_column_renamed = not renamed_sources.isdisjoint(constraint.local_columns)
    self_reference_renamed = (
        constraint.referenced_table == table_name
        and not renamed_sources.isdisjoint(constraint.referenced_columns)
    )
    return local_column_renamed or self_reference_renamed
