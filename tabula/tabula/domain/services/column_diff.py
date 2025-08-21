from __future__ import annotations

from collections.abc import Iterable

from tabula.domain.plan.actions import Action, AddColumn, DropColumn
from tabula.domain.model.column import Column


def diff_columns_for_adds(
    desired: Iterable[Column], observed: Iterable[Column]
) -> tuple[AddColumn, ...]:
    """Emit AddColumn for names present in desired but missing in observed. Preserve desired order."""
    observed_names = {c.name for c in observed}
    adds: list[AddColumn] = []
    for col in desired:
        if col.name not in observed_names:
            adds.append(AddColumn(column=col))
    return tuple(adds)


def diff_columns_for_drops(
    desired: Iterable[Column], observed: Iterable[Column]
) -> tuple[DropColumn, ...]:
    """Emit DropColumn for names present in observed but missing in desired. Sort by name deterministically."""
    desired_names = {c.name for c in desired}
    drops: list[DropColumn] = []
    for col in observed:
        if col.name not in desired_names:
            drops.append(DropColumn(column_name=col.name))
    return tuple(drops)


def diff_columns(desired: Iterable[Column], observed: Iterable[Column]) -> tuple[Action, ...]:
    """Compute add/drop actions only. Type/nullable changes are intentionally out-of-scope for now."""
    adds = diff_columns_for_adds(desired, observed)
    drops = diff_columns_for_drops(desired, observed)
    return adds + drops
