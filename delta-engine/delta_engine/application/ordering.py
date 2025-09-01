"""
Deterministic ordering for action plans.

Orders by phase (create → set-prop → add → drop → unset), then by subject
name. Target-level ordering is currently not part of the key.
"""

from typing import Final

from delta_engine.domain.plan import (
    Action,
    AddColumn,
    CreateTable,
    DropColumn,
    SetProperty,
    UnsetProperty,
)


def subject_name(action: Action) -> str:
    """Return the subject identifier used for ordering (e.g., a column name)."""
    if isinstance(action, AddColumn):
        return action.column.name
    if isinstance(action, DropColumn):
        return action.column_name
    if isinstance(action, SetProperty):
        return action.name.casefold()
    if isinstance(action, UnsetProperty):
        return action.name.casefold()
    return getattr(action, "name", "")


_PHASE_ORDER: Final[tuple[type[Action], ...]] = (
    CreateTable,
    SetProperty,
    AddColumn,
    DropColumn,
    UnsetProperty,
)

_PHASE_RANK: Final[dict[type[Action], int]] = {cls: i for i, cls in enumerate(_PHASE_ORDER)}


def action_sort_key(action: Action) -> tuple[int, str]:
    """
    Return a sortable key for action ordering.

    Orders by phase, then by subject name to achieve deterministic planning.
    """
    try:
        rank = _PHASE_RANK[type(action)]
    except KeyError as exc:
        raise ValueError(f"Place {type(action).__name__} in PHASE_ORDER.") from exc
    return (rank, subject_name(action))
