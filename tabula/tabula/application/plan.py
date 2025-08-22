"""Plan builder: compute ActionPlan and produce both machine and human summaries."""

from __future__ import annotations

from tabula.application.ports import CatalogReader
from tabula.application.results import PlanPreview
from tabula.application.order_plan import order_plan
from tabula.domain.model import DesiredTable
from tabula.domain.services.differ import diff



def plan_actions(desired_table: DesiredTable, reader: CatalogReader) -> PlanPreview:
    observed = reader.fetch_state(desired_table.qualified_name)
    plan = diff(observed, desired_table)
    is_noop = not plan
    summary_counts = plan.count_by_action()
    total_actions = len(plan)
    ordered_plan = order_plan(plan)
    
    return PlanPreview(
        plan=ordered_plan,
        is_noop=is_noop,
        summary_counts=summary_counts,
        total_actions=total_actions,
    )
