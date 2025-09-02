"""Utilities for diffing table property mappings."""

from __future__ import annotations

from collections.abc import Mapping

from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import (
    Action,
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


# returning tuple even though there is only ever 1 table comment.
# keeps it consistent with other actions
def diff_table_comments(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[SetTableComment, ...]:
    if desired.comment == observed.comment:
        return ()
    else:
        return (SetTableComment(comment=desired.comment),)
