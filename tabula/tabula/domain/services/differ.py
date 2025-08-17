from __future__ import annotations
from typing import Optional
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.table_state import TableState
from tabula.domain.model.actions import ActionPlan, CreateTable
from tabula.domain.services.column_diff import diff_columns

def table_exists(observed: Optional[TableState]) -> bool:
    """Existence is simply 'observed is not None'."""
    return observed is not None

def diff(observed: Optional[TableState], spec: TableSpec) -> ActionPlan:
    if observed is None:
        # Create table: actions are table-scoped; identity comes from the plan.
        return ActionPlan(spec.full_name, (CreateTable(columns=spec.columns),))

    if observed.full_name != spec.full_name:
        raise ValueError("spec.full_name and observed.full_name must match")

    actions = diff_columns(spec.columns, observed.columns)
    return ActionPlan(spec.full_name, actions)