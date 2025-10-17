"""Utilities for diffing table property mappings."""

from __future__ import annotations

from collections.abc import Mapping

from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import (
    Action,
    PartitionBy,
    SetProperty,
    SetTableComment,
    UnsetProperty,
)


def _diff_properties_for_sets(
    desired: Mapping[str, str],
    observed: Mapping[str, str],
) -> tuple[SetProperty, ...]:
    """Set or update any property whose value differs or is missing."""
    actions: list[SetProperty] = []
    for name, desired_value in desired.items():
        if observed.get(name) != desired_value:
            actions.append(SetProperty(name=name, value=desired_value))
    return tuple(actions)


def _diff_properties_for_unsets(
    desired: Mapping[str, str],
    observed: Mapping[str, str],
) -> tuple[UnsetProperty, ...]:
    """Unset any property present in observed but not in desired."""
    extras = set(observed) - set(desired)
    return tuple(UnsetProperty(name=n) for n in extras)


def diff_properties(
    desired: Mapping[str, str],
    observed: Mapping[str, str],
) -> tuple[Action, ...]:
    """Return property-level actions to transform `observed` into `desired`."""
    sets = _diff_properties_for_sets(desired, observed)
    unsets = _diff_properties_for_unsets(desired, observed)
    return sets + unsets


def diff_table_comments(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[SetTableComment, ...]:
    """Return a comment update action when the desired table comment differs."""
    # Returns an empty tuple when comments are equal to keep plan composition
    # consistent with other diff functions that return tuples of actions.
    if desired.comment == observed.comment:
        return ()
    else:
        return (SetTableComment(comment=desired.comment),)


def diff_partition_columns(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[PartitionBy, ...]:
    """
    Return the desired parition columns if different from observed.

    Note: This is operation is currently disallowed. We surface this action only to warn the user.
    """
    if desired.partitioned_by == observed.partitioned_by:
        return ()
    else:
        return (PartitionBy(desired.partitioned_by),)
