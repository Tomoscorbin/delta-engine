from __future__ import annotations

from collections.abc import Iterable

from tabula.domain.plan.actions import Action, AddColumn, DropColumn
from tabula.domain.model import Column


def diff_columns_for_adds(
    desired: Iterable[Column], observed: Iterable[Column]
) -> tuple[AddColumn, ...]:
    """Emit AddColumn for names present in desired but missing in observed."""
    observed_names = {c.name for c in observed}
    return tuple(AddColumn(column=c) for c in desired if c.name not in observed_names)


def diff_columns_for_drops(
    desired: Iterable[Column], observed: Iterable[Column]
) -> tuple[DropColumn, ...]:
    """Emit DropColumn for names present in observed but missing in desired."""
    desired_names = {c.name for c in desired}
    observed_names = {c.name for c in observed}
    drop_names = observed_names - desired_names
    return tuple(DropColumn(column_name=name) for name in drop_names)


def diff_columns(desired: Iterable[Column], observed: Iterable[Column]) -> tuple[Action, ...]:
    """Compute add/drop actions only. Type/nullable changes are intentionally out-of-scope for now."""
    adds = diff_columns_for_adds(desired, observed)
    drops = diff_columns_for_drops(desired, observed)
    return adds + drops
