"""
Translate a table diff into the action plan that reconciles it.

`lower_diff` is a mechanical translation: each fact in the `TableDiff` maps to
the DDL action(s) that respond to it, and facts the engine does not act on
(type changes, partitioning changes, observed-only properties) lower to
nothing — validation has already judged them. Reconciliation *policy* (what to
set, what to unset, what to leave alone) lives here, stated once per
dimension; the diff itself carries no policy.

`compute_plan` composes the two stages — `diff_table` then `lower_diff` — for
callers that want desired+observed → plan in one step.
"""

from __future__ import annotations

from typing import assert_never

from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetTableTag,
)
from delta_engine.domain.plan.diff import (
    Added,
    Changed,
    ColumnChanged,
    ColumnDrift,
    Entry,
    ForeignKeyDrift,
    KeyValue,
    Removed,
    TableDiff,
    TableDrift,
    TableMissing,
    diff_table,
)


def lower_diff(diff: TableDiff) -> ActionPlan:
    """Translate a table diff into the plan of actions that reconciles it."""
    match diff:
        case TableMissing(desired=desired):
            return ActionPlan(_create_actions(desired))
        case TableDrift() as drift:
            return ActionPlan(_reconcile_actions(drift))
        case _:
            assert_never(diff)


def compute_plan(desired: DesiredTable, observed: ObservedTable | None) -> ActionPlan:
    """
    Compute the actions required to reach the desired state.

    The diff-then-lower composition: `diff_table` states the facts,
    `lower_diff` translates them. Callers that need the intermediate diff
    (e.g. to validate it) call the stages directly.
    """
    return lower_diff(diff_table(desired, observed))


def _create_actions(desired: DesiredTable) -> tuple[Action, ...]:
    """
    Actions that instantiate a full declaration on a missing table.

    CREATE TABLE carries columns, comments, properties, partitioning, and the
    primary key inline. Tags and foreign keys cannot be declared inline, so
    they follow as separate actions — a missing table has nothing observed, so
    every declared value is set and none is unset.
    """
    tag_actions: tuple[Action, ...] = tuple(
        SetTableTag(name=name, value=value) for name, value in desired.tags.items()
    )
    column_tag_actions: tuple[Action, ...] = tuple(
        SetColumnTag(column_name=column.name, name=name, value=value)
        for column in desired.columns
        for name, value in column.tags.items()
    )
    foreign_key_actions: tuple[Action, ...] = tuple(
        SetForeignKey(
            local_columns=foreign_key.local_columns,
            referenced_table=foreign_key.referenced_table,
            referenced_columns=foreign_key.referenced_columns,
            constraint_name=foreign_key.constraint_name,
        )
        for foreign_key in desired.foreign_keys
    )
    return (CreateTable(desired), *tag_actions, *column_tag_actions, *foreign_key_actions)


def _reconcile_actions(drift: TableDrift) -> tuple[Action, ...]:
    """Actions that reconcile an existing table, one dimension at a time."""
    actions: list[Action] = []
    for column_drift in drift.columns:
        actions.extend(_lower_column_drift(column_drift))
    if drift.table_comment is not None:
        actions.append(SetTableComment(comment=drift.table_comment.desired))
    actions.extend(_lower_properties(drift.properties))
    actions.extend(_lower_table_tags(drift.table_tags))
    # drift.partitioning lowers to nothing: partitioning changes are
    # unsupported, and validation has already rejected the drift.
    actions.extend(_lower_primary_key(drift.primary_key))
    for foreign_key_drift in drift.foreign_keys:
        actions.extend(_lower_foreign_key(foreign_key_drift))
    return tuple(actions)


def _lower_column_drift(drift: ColumnDrift) -> tuple[Action, ...]:
    """Translate one column's drift into its reconciling actions."""
    match drift:
        case Added(item=column):
            tag_actions = tuple(
                SetColumnTag(column_name=column.name, name=name, value=value)
                for name, value in column.tags.items()
            )
            return (AddColumn(column=column), *tag_actions)
        case Removed(item=column):
            # Dropping the column removes its comment and tags with it.
            return (DropColumn(column.name),)
        case ColumnChanged() as changed:
            return _lower_column_changed(changed)
        case _:
            assert_never(drift)


def _lower_column_changed(changed: ColumnChanged) -> tuple[Action, ...]:
    """
    Translate a changed column's sub-facts, each independently.

    The data_type sub-fact lowers to nothing: type migrations are unsupported,
    and validation has already rejected the drift.
    """
    actions: list[Action] = []
    if changed.nullability is not None:
        actions.append(
            SetColumnNullability(
                column_name=changed.column_name, nullable=changed.nullability.desired
            )
        )
    if changed.comment is not None:
        actions.append(SetColumnComment(changed.column_name, changed.comment.desired))
    for entry in changed.tags:
        match entry:
            case Added(item=pair):
                actions.append(
                    SetColumnTag(column_name=changed.column_name, name=pair.name, value=pair.value)
                )
            case Changed(desired=pair):
                actions.append(
                    SetColumnTag(column_name=changed.column_name, name=pair.name, value=pair.value)
                )
            case Removed(item=pair):
                actions.append(UnsetColumnTag(column_name=changed.column_name, name=pair.name))
            case _:
                assert_never(entry)
    return tuple(actions)


def _lower_properties(entries: tuple[Entry[KeyValue], ...]) -> tuple[Action, ...]:
    """
    Translate property entries under declared-subset semantics.

    The engine only manages keys the user declared: Added and Changed entries
    are set; a Removed entry (an observed-only key, e.g. one Databricks sets
    autonomously) is deliberately ignored, never unset.
    """
    actions: list[Action] = []
    for entry in entries:
        match entry:
            case Added(item=pair):
                actions.append(SetProperty(name=pair.name, value=pair.value))
            case Changed(desired=pair):
                actions.append(SetProperty(name=pair.name, value=pair.value))
            case Removed():
                pass
            case _:
                assert_never(entry)
    return tuple(actions)


def _lower_table_tags(entries: tuple[Entry[KeyValue], ...]) -> tuple[Action, ...]:
    """
    Translate table-tag entries under full-state semantics.

    The engine owns the complete tag set: Added and Changed entries are set,
    and a Removed entry (an observed-only tag) is unset — the deliberate
    contrast with `_lower_properties`.
    """
    actions: list[Action] = []
    for entry in entries:
        match entry:
            case Added(item=pair):
                actions.append(SetTableTag(name=pair.name, value=pair.value))
            case Changed(desired=pair):
                actions.append(SetTableTag(name=pair.name, value=pair.value))
            case Removed(item=pair):
                actions.append(UnsetTableTag(name=pair.name))
            case _:
                assert_never(entry)
    return tuple(actions)


def _lower_primary_key(entry: Entry[PrimaryKeyConstraint] | None) -> tuple[Action, ...]:
    """Translate the primary-key entry: set, drop, or drop-then-set."""
    match entry:
        case None:
            return ()
        case Added(item=primary_key):
            return (
                SetPrimaryKey(
                    columns=primary_key.columns, constraint_name=primary_key.constraint_name
                ),
            )
        case Removed():
            return (DropPrimaryKey(),)
        case Changed(desired=primary_key):
            return (
                DropPrimaryKey(),
                SetPrimaryKey(
                    columns=primary_key.columns, constraint_name=primary_key.constraint_name
                ),
            )
        case _:
            assert_never(entry)


def _lower_foreign_key(entry: ForeignKeyDrift) -> tuple[Action, ...]:
    """Translate one foreign-key entry: set the desired content or drop by name."""
    match entry:
        case Added(item=foreign_key):
            return (
                SetForeignKey(
                    local_columns=foreign_key.local_columns,
                    referenced_table=foreign_key.referenced_table,
                    referenced_columns=foreign_key.referenced_columns,
                    constraint_name=foreign_key.constraint_name,
                ),
            )
        case Removed(item=foreign_key):
            return (DropForeignKey(constraint_name=foreign_key.constraint_name),)
        case _:
            assert_never(entry)
