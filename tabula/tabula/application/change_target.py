"""Utilities for assembling change targets."""

from __future__ import annotations

from tabula.application.ports import CatalogReader
from tabula.domain.model import ChangeTarget, DesiredTable


def load_change_target(reader: CatalogReader, desired_table: DesiredTable) -> ChangeTarget:
    """Pair desired table definition with its observed catalog state.

    Args:
        reader: Catalog reader used to fetch observed state.
        desired_table: Target table definition.

    Returns:
        ChangeTarget combining desired and observed states.
    """

    observed = reader.fetch_state(desired_table.qualified_name)
    return ChangeTarget(desired=desired_table, observed=observed)
