"""Utilities for diffing table schemas."""

from __future__ import annotations

from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import ActionPlan, CreateTable
from delta_engine.domain.services.column_diff import diff_columns
from delta_engine.domain.services.property_diff import diff_properties


def diff_tables(desired: DesiredTable, observed: ObservedTable | None) -> ActionPlan:
    """
    Compute the actions required to reach the desired schema.

    Args:
        desired: Desired table definition.
        observed: Current table definition or ``None`` if the table is missing.

    Returns:
        Action plan describing the necessary changes.

    """
    # TODO: come up with better way to do this
    if observed is None:
        actions = (
            CreateTable(columns=desired.columns),
            *diff_properties(desired.properties, {}),
        )
    else:
        actions = diff_columns(desired.columns, observed.columns) + diff_properties(
            desired.properties, observed.properties
        )
    return ActionPlan(desired.qualified_name, actions)
