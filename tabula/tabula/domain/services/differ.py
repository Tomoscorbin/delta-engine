from __future__ import annotations

"""Utilities for diffing table schemas."""

from tabula.domain.model import DesiredTable, ObservedTable
from tabula.domain.plan.actions import ActionPlan, CreateTable
from tabula.domain.services.column_diff import diff_columns


def diff(observed: ObservedTable | None, desired: DesiredTable) -> ActionPlan:
    """Compute the actions required to reach the desired schema.

    Args:
        observed: Current table definition or ``None`` if the table is missing.
        desired: Desired table definition.

    Returns:
        Action plan describing the necessary changes.

    Raises:
        ValueError: If the qualified names of ``observed`` and ``desired`` differ.
    """

    if observed is None:
        return ActionPlan(desired.qualified_name, (CreateTable(columns=desired.columns),))

    if observed.qualified_name != desired.qualified_name:
        raise ValueError("qualified_name must match between desired and observed")

    actions = diff_columns(desired.columns, observed.columns)
    return ActionPlan(desired.qualified_name, actions)
