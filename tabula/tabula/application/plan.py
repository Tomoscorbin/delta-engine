"""Plan builder: compute ActionPlan and produce both machine and human summaries."""

from __future__ import annotations
from collections import Counter
from tabula.domain.model.table import DesiredTable
from tabula.domain.services.differ import diff
from tabula.application.ports import CatalogReader
from tabula.application.results import PlanPreview

def plan_actions(desired_table: DesiredTable, reader: CatalogReader) -> PlanPreview:
    observed = reader.fetch_state(desired_table.qualified_name)
    plan = diff(observed, desired_table)

    # Stable taxonomy via Action.kind (domain contract)
    counts = Counter(a.kind for a in plan)
    summary_text = " ".join(f"{k}={counts[k]}" for k in sorted(counts)) if counts else "noop"
    is_noop = not plan

    return PlanPreview(
        plan=plan,
        is_noop=is_noop,
        summary_counts=dict(counts),
        summary_text=summary_text,
    )
