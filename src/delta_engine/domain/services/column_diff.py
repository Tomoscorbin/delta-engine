"""Utilities for diffing column definitions."""

from __future__ import annotations

from delta_engine.domain.model import Column
from delta_engine.domain.plan.actions import (
    Action,
    AddColumn,
    DropColumn,
    SetColumnComment,
    SetColumnNullability,
)


def diff_columns(desired: tuple[Column, ...], observed: tuple[Column, ...]) -> tuple[Action, ...]:
    """Return the column-level actions to transform `observed` into `desired`."""
    adds = _diff_columns_for_adds(desired, observed)
    drops = _diff_columns_for_drops(desired, observed)
    comment_updates = _diff_column_comments(desired, observed)
    nullability_changes = _diff_column_nullability(desired, observed)
    return adds + drops + comment_updates + nullability_changes


def _diff_columns_for_adds(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> tuple[AddColumn, ...]:
    """Return AddColumn actions for columns present in desired but missing in observed."""
    observed_names = {column.name for column in observed}
    return tuple(
        AddColumn(column=column) for column in desired if column.name not in observed_names
    )


def _diff_columns_for_drops(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> tuple[DropColumn, ...]:
    """Return DropColumn actions for columns present in observed but missing in desired."""
    desired_names = {column.name for column in desired}
    return tuple(
        DropColumn(column.name) for column in observed if column.name not in desired_names
    )


def _diff_column_comments(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> tuple[SetColumnComment, ...]:
    """
    Return `SetColumnComment` actions for columns whose desired comment differs from observed.

    Note: Delta Engine sets column comments to '' by default.
    """
    observed_comment_by_name = {column.name: column.comment for column in observed}
    return tuple(
        SetColumnComment(column.name, column.comment)
        for column in desired
        if observed_comment_by_name.get(column.name) != column.comment
    )


def _diff_column_nullability(
    desired: tuple[Column, ...], observed: tuple[Column, ...]
) -> tuple[SetColumnNullability, ...]:
    """Return `SetColumnNullability` actions for columns whose `nullable` flag differs."""
    observed_nullability_by_name = {column.name: column.nullable for column in observed}
    return tuple(
        SetColumnNullability(column_name=column.name, nullable=column.nullable)
        for column in desired
        if observed_nullability_by_name.get(column.name) != column.nullable
    )
