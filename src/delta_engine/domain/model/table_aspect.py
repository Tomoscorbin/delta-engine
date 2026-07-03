"""
The independently manageable dimensions of a table's state.

Each :class:`TableAspect` member corresponds to exactly one diff dimension in
the planner: the differ reconciles an aspect only when the desired table
declares it managed. Declaration order is canonical — collections of aspects
(e.g. ``TargetColumnMissing.reasons``) list members in this order so messages
and tests are deterministic.
"""

from __future__ import annotations

from enum import Enum, auto
from typing import Final


class TableAspect(Enum):
    """One independently manageable dimension of a table's state."""

    COLUMN_STRUCTURE = auto()
    TABLE_COMMENT = auto()
    COLUMN_COMMENTS = auto()
    TABLE_TAGS = auto()
    COLUMN_TAGS = auto()
    PROPERTIES = auto()
    PARTITIONING = auto()
    PRIMARY_KEY = auto()
    FOREIGN_KEYS = auto()


ALL_ASPECTS: Final[frozenset[TableAspect]] = frozenset(TableAspect)
