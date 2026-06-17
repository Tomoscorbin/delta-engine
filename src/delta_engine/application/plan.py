"""
Compute and order action plans.

Provides `PlanContext` and `make_plan_context`, which diffs desired vs. observed
tables and returns an `ActionPlan` whose actions are sorted for deterministic
execution. Ordering is by execution phase, then by subject name within a phase;
both are declared by each action type (see :class:`Action`), so the sort key
stays agnostic of concrete action types -- a new action orders itself.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan import Action, ActionPlan
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


def action_sort_key(action: Action) -> tuple[int, str]:
    """Return the deterministic ordering key: execution phase, then subject name."""
    return (action.phase, action.subject)


def make_plan_context(desired: DesiredTable, observed: ObservedTable | None) -> PlanContext:
    """Create a :class:`PlanContext` for a pair of tables and its ordered plan."""
    unsorted_plan = diff_tables(desired=desired, observed=observed)
    ordered_actions = tuple(sorted(unsorted_plan.actions, key=action_sort_key))
    plan = replace(unsorted_plan, actions=ordered_actions)
    return PlanContext(desired=desired, observed=observed, plan=plan)
