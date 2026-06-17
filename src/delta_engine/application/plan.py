"""
Compute action plans.

Provides `PlanContext` and `make_plan_context`, which diffs desired vs. observed
tables into an `ActionPlan`. The plan orders its own actions for deterministic
execution (see :class:`ActionPlan`), so this module just diffs and bundles.
"""

from __future__ import annotations

from dataclasses import dataclass

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
        plan: The computed action plan to reach ``desired``.

    """

    desired: DesiredTable
    observed: ObservedTable | None
    plan: ActionPlan


def make_plan_context(desired: DesiredTable, observed: ObservedTable | None) -> PlanContext:
    """Create a :class:`PlanContext` for a pair of tables and its plan."""
    plan = diff_tables(desired=desired, observed=observed)
    return PlanContext(desired=desired, observed=observed, plan=plan)
