from __future__ import annotations
from typing import Iterable, Tuple
from tabula.domain.model.column import Column
from tabula.domain.model.actions import AddColumn, DropColumn, Action

def diff_columns_for_adds(desired: Iterable[Column], observed: Iterable[Column]) -> Tuple[AddColumn, ...]:
    """Emit AddColumn for names present in desired but missing in observed. Preserve desired order."""
    observed_names = {c.name for c in observed}
    adds: list[AddColumn] = []
    for c in desired:
        if c.name not in observed_names:
            adds.append(AddColumn(column=c))
    return tuple(adds)

def diff_columns_for_drops(desired: Iterable[Column], observed: Iterable[Column]) -> Tuple[DropColumn, ...]:
    """Emit DropColumn for names present in observed but missing in desired. Sort by name deterministically."""
    desired_names = {c.name for c in desired}
    drops = [DropColumn(column_name=c.name) for c in observed if c.name not in desired_names]
    drops.sort(key=lambda a: a.column_name)
    return tuple(drops)

def diff_columns(desired: Iterable[Column], observed: Iterable[Column]) -> Tuple[Action, ...]:
    """Compute add/drop actions only. Type/nullable changes are intentionally out-of-scope for now."""
    adds = diff_columns_for_adds(desired, observed)
    drops = diff_columns_for_drops(desired, observed)
    return (adds + drops) if adds or drops else ()
