"""Utilities for ordering action plans."""

from __future__ import annotations

from dataclasses import replace

from tabula.domain.plan.actions import Action, ActionPlan, AddColumn, CreateTable, DropColumn


# Single source of truth for order
def _phase_rank(a: Action) -> int:
    match a:
        case CreateTable():
            return 0
        case AddColumn():
            return 1
        case DropColumn():
            return 2
        case _:
            return 99  # unknowns sink to the end


def order_plan(plan: ActionPlan) -> ActionPlan:
    """Return a new plan ordered by execution phase.

    Args:
        plan: Plan to reorder.

    Returns:
        New ``ActionPlan`` with actions sorted for safe execution.
    """

    if not plan:
        return plan
    ordered = tuple(sorted(plan, key=_phase_rank))
    return replace(plan, actions=ordered)
