from __future__ import annotations

from tabula.domain.plan.actions import ActionPlan, CreateTable
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.services.column_diff import diff_columns


def table_exists(observed: ObservedTable | None) -> bool:
    """Existence is simply 'observed is not None'."""
    return observed is not None


def diff(observed: ObservedTable | None, desired: DesiredTable) -> ActionPlan:
    if observed is None:
        return ActionPlan(desired.qualified_name, (CreateTable(columns=desired.columns),))
    if observed.qualified_name != desired.qualified_name:
        raise ValueError("qualified_name must match between desired and observed")
    return ActionPlan(desired.qualified_name, diff_columns(desired.columns, observed.columns))
