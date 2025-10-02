"""Utilities for diffing table schemas."""

from __future__ import annotations

from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import Action, ActionPlan, CreateTable
from delta_engine.domain.services.column_diff import diff_columns
from delta_engine.domain.services.table_diff import (
    diff_partition_columns,
    diff_properties,
    diff_table_comments,
)


def diff_tables(desired: DesiredTable, observed: ObservedTable | None) -> ActionPlan:
    """
    Compute the actions required to reach the desired schema.

    Args:
        desired: Desired table definition.
        observed: Current table definition or ``None`` if the table is missing.

    Returns:
        Action plan describing the necessary changes.

    """
    actions: tuple[Action, ...]
    if observed is None:
        actions = (CreateTable(desired),)
    else:
        actions = (
            diff_columns(desired.columns, observed.columns)
            + diff_properties(desired.properties, observed.properties)
            + diff_table_comments(desired, observed)
            + diff_partition_columns(desired, observed)
        )
    return ActionPlan(desired.qualified_name, actions)
