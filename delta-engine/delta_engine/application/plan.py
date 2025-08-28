"""Utilities for ordering action plans."""

from __future__ import annotations

from dataclasses import dataclass, replace

from delta_engine.application.ordering import action_sort_key
from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan import ActionPlan
from delta_engine.domain.services.differ import diff_tables


@dataclass(frozen=True, slots=True)
class PlanContext:
    desired: DesiredTable
    observed: ObservedTable | None
    plan: ActionPlan


def make_plan_context(observed: ObservedTable | None, desired: DesiredTable) -> PlanContext:
    plan = _compute_plan(observed, desired)
    return PlanContext(
        desired=desired,
        observed=observed,
        plan=plan,
    )


def _compute_plan(observed, desired) -> ActionPlan:
    """Create a sorted ActionPlan from observed/desired."""
    unsorted_plan = diff_tables(desired=desired, observed=observed)
    sorted_actions = tuple(sorted(unsorted_plan.actions, key=action_sort_key))
    return replace(unsorted_plan, actions=sorted_actions)
