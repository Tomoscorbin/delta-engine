"""Utilities for diffing table property mappings."""

from __future__ import annotations

from collections.abc import Mapping

from delta_engine.domain.plan.actions import Action, SetProperty, UnsetProperty


def diff_properties_for_sets(
    desired: Mapping[str, str],
    observed: Mapping[str, str],
) -> tuple[SetProperty, ...]:
    """Set or update any property whose value differs or is missing."""
    actions: list[SetProperty] = []
    for name, desired_value in desired.items():
        if observed.get(name) != desired_value:
            actions.append(SetProperty(name=name, value=desired_value))
    return tuple(actions)


def diff_properties_for_unsets(
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
    sets = diff_properties_for_sets(desired, observed)
    unsets = diff_properties_for_unsets(desired, observed)
    return sets + unsets
