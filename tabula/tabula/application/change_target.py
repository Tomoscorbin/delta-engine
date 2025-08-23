from __future__ import annotations

from tabula.application.ports import CatalogReader
from tabula.domain.model import ChangeTarget, DesiredTable


def load_change_target(reader: CatalogReader, desired_table: DesiredTable) -> ChangeTarget:
    """Read the observed state from the catalog and pair it with the desired state."""
    observed = reader.fetch_state(desired_table.qualified_name)
    return ChangeTarget(desired=desired_table, observed=observed)
