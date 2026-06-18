"""
Compute action plans.

`plan_table` diffs a desired table against the observed one and returns an
`ActionPlan`. The plan orders its own actions for deterministic execution (see
:class:`ActionPlan`), so this is a thin, named wrapper over the differ.
"""

from __future__ import annotations

from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan import ActionPlan
from delta_engine.domain.plan.differ import diff_tables


def plan_table(desired: DesiredTable, observed: ObservedTable | None) -> ActionPlan:
    """Diff ``desired`` against the ``observed`` state and return the action plan."""
    return diff_tables(desired=desired, observed=observed)
