"""Validated boundary from complete table diffs to executable action plans."""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import assert_never

from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import validate_diff
from delta_engine.domain.model import DesiredTable, QualifiedName, identifier_key
from delta_engine.domain.plan import (
    Action,
    ActionPlan,
    AlterClustering,
    CreateTable,
    SetForeignKey,
    SetPrimaryKey,
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

type ResultingSchemas = Mapping[QualifiedName, Mapping[str, str]]


def plan_diff(diff: TableDiff, resulting_schemas: ResultingSchemas) -> PlanningResult:
    """
    Validate ``diff`` and return an accepted (bound) or rejected result.

    This is the only boundary that constructs an :class:`ActionPlan` from a
    complete diff. A rejected result carries validation failures and
    deliberately has no plan, making execution of unvalidated drift
    unrepresentable. Accepted actions are bound before the plan is built:
    symbolic column references (primary keys, foreign keys, clustering, and
    a created table's internal references) are resolved through
    ``resulting_schemas`` to the exact post-sync spelling, so the plan is
    self-contained and compilation stays mechanical. The plan carries the
    relation kind its actions lower against: the observed kind for drift,
    and the default ordinary kind for a creation.
    """
    validation = validate_diff(diff)
    if validation.failed:
        return PlanningFailed(failures=validation.failures)
    bound_actions = _bind_actions(diff, resulting_schemas)
    match diff:
        case TableDrift() as drift:
            plan = ActionPlan(
                target=drift.target,
                actions=bound_actions,
                kind=drift.observed.kind,
            )
        case TableMissing() as missing:
            plan = ActionPlan(
                target=missing.target,
                actions=bound_actions,
            )
        case _ as unreachable:
            assert_never(unreachable)
    return PlanningSucceeded(plan=plan)


def _bind_actions(diff: TableDiff, resulting_schemas: ResultingSchemas) -> tuple[Action, ...]:
    """Bind every accepted action's symbolic references to post-sync spelling."""
    own = resulting_schemas.get(diff.target)
    if own is None:
        raise RuntimeError(
            f"No resulting schema for planned table {diff.target}; the engine"
            " derives one for every diffed table"
        )
    return tuple(_bind_action(action, own, resulting_schemas) for action in diff.actions)


def _bind_action(
    action: Action,
    own: Mapping[str, str],
    resulting_schemas: ResultingSchemas,
) -> Action:
    """Return ``action`` with symbolic column references bound, or unchanged."""
    match action:
        case SetPrimaryKey(primary_key=primary_key):
            return SetPrimaryKey(
                primary_key=replace(
                    primary_key,
                    columns=tuple(_own_spelling(own, name) for name in primary_key.columns),
                )
            )
        case AlterClustering():
            return replace(
                action,
                desired_clustering=tuple(
                    _own_spelling(own, name) for name in action.desired_clustering
                ),
            )
        case SetForeignKey(constraint=constraint):
            parent = resulting_schemas.get(constraint.referenced_table)
            return SetForeignKey(
                constraint=replace(
                    constraint,
                    local_columns=tuple(
                        _own_spelling(own, name) for name in constraint.local_columns
                    ),
                    referenced_columns=tuple(
                        _parent_spelling(parent, name) for name in constraint.referenced_columns
                    ),
                )
            )
        case CreateTable(table=table):
            return CreateTable(table=_bind_created_table(table, own))
        case _:
            return action


def _bind_created_table(table: DesiredTable, own: Mapping[str, str]) -> DesiredTable:
    """
    Bind a created table's internal primary-key and layout references.

    The table's ``foreign_keys`` are deliberately untouched: CREATE TABLE
    renders no foreign keys — the separate ``SetForeignKey`` actions carry
    the bound, executable constraints.
    """
    bound_primary_key = (
        replace(
            table.primary_key,
            columns=tuple(_own_spelling(own, name) for name in table.primary_key.columns),
        )
        if table.primary_key is not None
        else None
    )
    return replace(
        table,
        primary_key=bound_primary_key,
        partitioned_by=tuple(_own_spelling(own, name) for name in table.partitioned_by),
        clustered_by=tuple(_own_spelling(own, name) for name in table.clustered_by),
    )


def _own_spelling(own: Mapping[str, str], name: str) -> str:
    """Resolve an own-table reference; a miss is an engine invariant violation."""
    spelling = own.get(identifier_key(name))
    if spelling is None:
        raise RuntimeError(
            f"Accepted action references no resulting column: {name!r}."
            " Declaration validation makes this unreachable short of an engine defect."
        )
    return spelling


def _parent_spelling(parent: Mapping[str, str] | None, name: str) -> str:
    """
    Resolve a foreign key's referenced column to the parent's post-sync spelling.

    An unregistered, read-failed, or divergent parent legitimately cannot
    bind, so any miss falls back to the declared spelling: the child still
    compiles preview SQL, and dependency resolution owns classifying the
    failure and blocking execution.
    """
    if parent is None:
        return name
    return parent.get(identifier_key(name), name)
