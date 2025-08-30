"""Deterministic ordering for action plans.

Defines a sort key that orders actions by phase (create, add, drop), then by
target name, then by subject name to yield stable, readable plans.
"""

from typing import Final

from delta_engine.domain.plan import Action, AddColumn, CreateTable, DropColumn


def target_name(action: Action) -> str:
    """Return the target identifier used for ordering."""
    return str(getattr(action, "target", ""))


def subject_name(action: Action) -> str:
    """Return the subject identifier used for ordering (e.g., column name)."""
    if isinstance(action, AddColumn):
        return action.column.name
    if isinstance(action, DropColumn):
        return action.column_name
    return getattr(action, "name", "")


_PHASE_ORDER: Final[tuple[type[Action], ...]] = (
    CreateTable,
    AddColumn,
    DropColumn,
)

_PHASE_RANK: Final[dict[type[Action], int]] = {cls: i for i, cls in enumerate(_PHASE_ORDER)}


def action_sort_key(action: Action) -> tuple[int, str, str]:
    """
    Return a sortable key for action ordering.

    Orders by phase (create, add, drop), then by target name, then by subject
    name to achieve deterministic planning.
    """
    try:
        rank = _PHASE_RANK[type(action)]
    except KeyError as exc:
        raise ValueError(f"Place {type(action).__name__} in PHASE_ORDER.") from exc
    return (rank, target_name(action), subject_name(action))
