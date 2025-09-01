"""Utilities for diffing column definitions."""

from __future__ import annotations

from delta_engine.domain.model import Column
from delta_engine.domain.plan.actions import Action, AddColumn, DropColumn, SetColumnComment


def diff_columns_for_adds(desired: tuple[Column], observed: tuple[Column]) -> tuple[AddColumn]:
    """Return AddColumn actions for columns present in desired but missing in observed."""
    observed_names = {c.name for c in observed}
    return tuple(AddColumn(column=c) for c in desired if c.name not in observed_names)


def diff_columns_for_drops(desired: tuple[Column], observed: tuple[Column]) -> tuple[DropColumn]:
    """Return DropColumn actions for columns present in observed but missing in desired."""
    desired_names = {c.name for c in desired}
    return tuple(DropColumn(c.name) for c in observed if c.name not in desired_names)


def diff_columns_for_comment_updates(
    desired: tuple[Column], observed: tuple[Column]
) -> tuple[SetColumnComment]:
    # TODO: perhaps add comments with AddColumn and skip SetColumnComment if column is new

    observed_comment_by_name = {c.name: c.comment for c in observed}
    return tuple(
        SetColumnComment(c.name, c.comment)
        for c in desired
        if observed_comment_by_name.get(c.name) != c.comment
    )


def diff_columns(desired: tuple[Column], observed: tuple[Column]) -> tuple[Action]:
    """Return the column-level actions to transform `observed` into `desired`."""
    adds = diff_columns_for_adds(desired, observed)
    drops = diff_columns_for_drops(desired, observed)
    comment_updates = diff_columns_for_comment_updates(desired, observed)
    return adds + drops + comment_updates
