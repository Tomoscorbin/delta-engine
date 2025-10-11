"""
Compute and order action plans.

Provides `PlanContext` and helpers to diff desired vs. observed tables and
return an `ActionPlan` with actions sorted for deterministic execution.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace

from delta_engine.application.ordering import action_sort_key
from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan import Action, ActionPlan
from delta_engine.domain.services.differ import diff_tables

SortKey = Callable[[Action], tuple[int, str]]


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


def make_plan_context(
    desired: DesiredTable,
    observed: ObservedTable | None,
    sort_key: SortKey = action_sort_key,
) -> PlanContext:
    """Create a :class:`PlanContext` for a pair of tables and its plan."""
    unsorted_plan = diff_tables(desired=desired, observed=observed)
    plan = _order_actions(unsorted_plan, sort_key=sort_key)
    return PlanContext(desired=desired, observed=observed, plan=plan)


def _order_actions(plan: ActionPlan, sort_key: SortKey) -> ActionPlan:
    """Return a new ActionPlan whose actions are deterministically ordered by sort_key."""
    return replace(plan, actions=tuple(sorted(plan.actions, key=sort_key)))
