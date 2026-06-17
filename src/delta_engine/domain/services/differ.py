"""
Compute the actions required to reconcile a table to its desired schema.

`diff_tables` is the single public entry point: given the desired definition and
the currently observed one (or ``None`` when the table is missing), it returns
the `ActionPlan` that closes the gap. The per-dimension diffs (columns,
properties, table comment) are private helpers — they exist only to keep
`diff_tables` readable and have no meaning outside it.
"""

from __future__ import annotations

from collections.abc import Mapping

from delta_engine.domain.model import Column, DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetColumnNullability,
    SetProperty,
    SetTableComment,
)


def diff_tables(desired: DesiredTable, observed: ObservedTable | None) -> ActionPlan:
    """
    Compute the actions required to reach the desired schema.

    Args:
        desired: Desired table definition.
        observed: Current table definition or ``None`` if the table is missing.

    Returns:
        Action plan describing the necessary changes.

    """
    if observed is None:
        actions: tuple[Action, ...] = (CreateTable(desired),)
    else:
        actions = (
            _diff_columns(desired.columns, observed.columns)
            + _diff_properties(desired.properties, observed.properties)
            + _diff_table_comment(desired.comment, observed.comment)
        )
    return ActionPlan(desired.qualified_name, actions)


def _diff_columns(desired: tuple[Column, ...], observed: tuple[Column, ...]) -> tuple[Action, ...]:
    """Return the column-level actions to transform `observed` into `desired`."""
    observed_by_name = {column.name: column for column in observed}
    desired_names = {column.name for column in desired}

    added = tuple(column for column in desired if column.name not in observed_by_name)
    dropped = tuple(column for column in observed if column.name not in desired_names)
    common = tuple(
        (desired_column, observed_by_name[desired_column.name])
        for desired_column in desired
        if desired_column.name in observed_by_name
    )

    add_actions = tuple(AddColumn(column=column) for column in added)
    drop_actions = tuple(DropColumn(column.name) for column in dropped)
    comment_actions = tuple(
        SetColumnComment(desired_column.name, desired_column.comment)
        for desired_column, observed_column in common
        if desired_column.comment != observed_column.comment
    )
    nullability_actions = tuple(
        SetColumnNullability(column_name=desired_column.name, nullable=desired_column.nullable)
        for desired_column, observed_column in common
        if desired_column.nullable != observed_column.nullable
    )
    return add_actions + drop_actions + comment_actions + nullability_actions


def _diff_properties(
    desired: Mapping[str, str], observed: Mapping[str, str]
) -> tuple[SetProperty, ...]:
    """
    Return the `SetProperty` actions needed to align observed with desired.

    Properties are a declared subset, not a complete desired state: the engine
    only manages keys the user declared. Observed-only keys (e.g. properties
    Databricks sets autonomously) are left untouched — they are never unset.
    """
    return tuple(
        SetProperty(name=name, value=value)
        for name, value in desired.items()
        if observed.get(name) != value
    )


def _diff_table_comment(desired: str, observed: str) -> tuple[SetTableComment, ...]:
    """Return a comment update action when the desired table comment differs."""
    if desired == observed:
        return ()
    return (SetTableComment(comment=desired),)
