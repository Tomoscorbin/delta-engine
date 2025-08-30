"""
Compute and order action plans.

Provides `PlanContext` and helpers to diff desired vs. observed tables and
return an `ActionPlan` with actions sorted for deterministic execution.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from delta_engine.application.ordering import action_sort_key
from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan import ActionPlan
from delta_engine.domain.services.differ import diff_tables


@dataclass(frozen=True, slots=True)
class PlanContext:
    """
    Planning context capturing desired/observed state and the action plan.

    Attributes:
        desired: The user-authored desired definition.
        observed: The currently observed definition or ``None`` if missing.
        plan: The computed and ordered action plan to reach ``desired``.

    """

    desired: DesiredTable
    observed: ObservedTable | None
    plan: ActionPlan


def make_plan_context(desired: DesiredTable, observed: ObservedTable | None) -> PlanContext:
    """Create a :class:`PlanContext` for a pair of tables and its plan."""
    plan = _compute_plan(observed, desired)
    return PlanContext(
        desired=desired,
        observed=observed,
        plan=plan,
    )


def _compute_plan(desired: DesiredTable, observed: ObservedTable | None) -> ActionPlan:
    """Create a sorted :class:`ActionPlan` from observed and desired states."""
    unsorted_plan = diff_tables(desired=desired, observed=observed)
    sorted_actions = tuple(sorted(unsorted_plan.actions, key=action_sort_key))
    return replace(unsorted_plan, actions=sorted_actions)
