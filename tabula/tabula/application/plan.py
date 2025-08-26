"""Utilities for ordering action plans."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace

from tabula.application.ordering import action_sort_key
from tabula.domain.model import DesiredTable, ObservedTable, QualifiedName
from tabula.domain.plan import ActionPlan
from tabula.domain.services.differ import diff_tables


@dataclass(frozen=True, slots=True)
class PlanContext:
    qualified_name: QualifiedName
    desired: DesiredTable
    observed: ObservedTable | None
    plan: ActionPlan


def compute_plan(*, observed, desired) -> ActionPlan:
    """Create a sorted ActionPlan from observed/desired."""
    unsorted_plan = diff_tables(desired=desired, observed=observed)
    sorted_actions = tuple(sorted(unsorted_plan.actions, key=action_sort_key))
    return replace(unsorted_plan, actions=sorted_actions)

def plan_summary_counts(plan: ActionPlan) -> dict[str, int]:
    def action_name(a: object) -> str:
        return getattr(a, "action_name", type(a).__name__)
    return dict(Counter(action_name(a) for a in plan.actions))


def make_plan_context(observed: ObservedTable | None, desired: DesiredTable) -> PlanContext:
    plan = compute_plan(observed, desired)
    return PlanContext(
        qualified_name=desired.qualified_name,
        desired=desired,
        observed=observed,
        plan=plan,
    )
