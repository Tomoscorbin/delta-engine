from __future__ import annotations

from tabula.domain.plan.actions import ActionPlan, CreateTable
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.services.column_diff import diff_columns


def diff(observed: ObservedTable | None, desired: DesiredTable) -> ActionPlan:
    """
    Produce an ActionPlan to move the observed table to the desired schema.

    Rules:
      - If observed is None: create the table with all desired columns.
      - If qualified names differ: hard error.
      - Otherwise: only column adds/drops (type/nullability diffs are ignored here).
    """
    if observed is None:
        return ActionPlan(desired.qualified_name, (CreateTable(columns=desired.columns),))

    if observed.qualified_name != desired.qualified_name:
        raise ValueError("qualified_name must match between desired and observed")

    actions = diff_columns(desired.columns, observed.columns)
    return ActionPlan(desired.qualified_name, actions)