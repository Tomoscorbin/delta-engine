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
    observed_by_name = {column.name: column for column in observed}
    desired_names = {column.name for column in desired}

    added = tuple(column for column in desired if column.name not in observed_by_name)
    dropped = tuple(column for column in observed if column.name not in desired_names)
    common = tuple(
        (desired_column, observed_by_name[desired_column.name])
        for desired_column in desired
        if desired_column.name in observed_by_name
    )

    return (
        tuple(AddColumn(column=column) for column in added)
        + tuple(DropColumn(column.name) for column in dropped)
        + tuple(
            SetColumnComment(desired_column.name, desired_column.comment)
            for desired_column, observed_column in common
            if desired_column.comment != observed_column.comment
        )
        + tuple(
            SetColumnNullability(column_name=desired_column.name, nullable=desired_column.nullable)
            for desired_column, observed_column in common
            if desired_column.nullable != observed_column.nullable
        )
    )
