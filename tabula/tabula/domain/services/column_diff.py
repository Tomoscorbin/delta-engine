from __future__ import annotations
from typing import Iterable, Tuple
from tabula.domain.model.column import Column
from tabula.domain.model.actions import AddColumn, DropColumn, Action

def diff_columns_for_adds(
    desired_columns: Iterable[Column],
    observed_columns: Iterable[Column],
) -> Tuple[AddColumn, ...]:
    """
    Return AddColumn actions for columns that are in desired but not observed.
    Preserves the order from desired_columns.
    """
    observed_names = {c.name for c in observed_columns}
    adds: list[AddColumn] = []
    for column in desired_columns:
        if column.name not in observed_names:
            adds.append(AddColumn(column=column))
    return tuple(adds)

def diff_columns_for_drops(
    desired_columns: Iterable[Column],
    observed_columns: Iterable[Column],
) -> Tuple[DropColumn, ...]:
    """
    Return DropColumn actions for columns that are in observed but not desired.
    Sorts by column name (case-insensitive) for stable, readable output.
    """
    desired_names = {c.name for c in desired_columns}
    drops: list[DropColumn] = []
    for column in observed_columns:
        if column.name not in desired_names:
            drops.append(DropColumn(column_name=column.name))
    drops.sort(key=lambda a: a.column_name.casefold())  # deterministic
    return tuple(drops)

def diff_columns(
    desired_columns: Iterable[Column],
    observed_columns: Iterable[Column],
) -> Tuple[Action, ...]:
    """
    Compute add/drop actions. Type changes are out-of-scope for the MVP.
    Adds are emitted in spec order; drops are name-sorted.
    """
    adds = diff_columns_for_adds(desired_columns, observed_columns)
    drops = diff_columns_for_drops(desired_columns, observed_columns)
    return tuple(adds) + tuple(drops)