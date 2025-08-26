"""Utilities for diffing column definitions."""

from __future__ import annotations

from collections.abc import Iterable

from delta_engine.domain.model import Column
from delta_engine.domain.plan.actions import Action, AddColumn, DropColumn


def diff_columns_for_adds(
    desired: Iterable[Column], observed: Iterable[Column]
) -> tuple[AddColumn, ...]:
    """Return AddColumn actions for columns present in desired but missing in observed."""
    observed_names = {c.name for c in observed}
    return tuple(AddColumn(column=c) for c in desired if c.name not in observed_names)


def diff_columns_for_drops(
    desired: Iterable[Column], observed: Iterable[Column]
) -> tuple[DropColumn, ...]:
    """Return DropColumn actions for columns present in observed but missing in desired."""
    desired_names = {c.name for c in desired}
    return tuple(DropColumn(name=c.name) for c in observed if c.name not in desired_names)


def diff_columns(
    desired: Iterable[Column], observed: Iterable[Column]
) -> tuple[Action, ...]:
    """Return the column-level actions to transform `observed` into `desired`."""
    adds = diff_columns_for_adds(desired, observed)
    drops = diff_columns_for_drops(desired, observed)
    return adds + drops
