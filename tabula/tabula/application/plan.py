from __future__ import annotations
import re
from collections import Counter
from typing import Optional
from tabula.application.ports import CatalogReader
from tabula.application.results import PlanPreview
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.table_state import TableState
from tabula.domain.model.actions import ActionPlan
from tabula.domain.services.differ import diff

def _to_snake(name: str) -> str:
    return re.sub(r'(?<!^)([A-Z])', r'_\1', name).lower()

def _summarize(plan: ActionPlan) -> str:
    counts = Counter(_to_snake(type(a).__name__) for a in plan.actions)
    return " ".join(f"{k}={counts[k]}" for k in sorted(counts)) or "noop"

def plan_actions(spec: TableSpec, reader: CatalogReader) -> PlanPreview:
    observed: Optional[TableState] = reader.fetch_state(spec.full_name)
    plan: ActionPlan = diff(observed, spec)
    summary = _summarize(plan)
    return PlanPreview(plan=plan, is_noop=(len(plan) == 0), summary=summary)