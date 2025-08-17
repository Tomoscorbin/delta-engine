from __future__ import annotations
from typing import Optional
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.model.actions import ActionPlan, CreateTable
from tabula.domain.services.column_diff import diff_columns

def table_exists(observed: Optional[ObservedTable]) -> bool:
    """Existence is simply 'observed is not None'."""
    return observed is not None

def diff(observed: Optional[ObservedTable], desired: DesiredTable) -> ActionPlan:
    if observed is None:
        return ActionPlan(desired.qualified_name, (CreateTable(columns=desired.columns),))
    if observed.qualified_name != desired.qualified_name:
        raise ValueError("qualified_name must match between desired and observed")
    return ActionPlan(desired.qualified_name, diff_columns(desired.columns, observed.columns))
