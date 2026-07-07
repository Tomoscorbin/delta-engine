"""The independently manageable dimensions of a table's state."""

from enum import Enum, auto
from typing import Final


class TableAspect(Enum):
    """One independently manageable dimension of a table's state."""

    COLUMN_STRUCTURE = auto()
    COLUMN_COMMENTS = auto()
    COLUMN_TAGS = auto()
    TABLE_COMMENT = auto()
    TABLE_TAGS = auto()
    PROPERTIES = auto()
    PARTITIONING = auto()
    PRIMARY_KEY = auto()
    FOREIGN_KEYS = auto()

    @property
    def label(self) -> str:
        """Human-readable label (e.g. COLUMN_STRUCTURE -> 'column structure')."""
        return self.name.lower().replace("_", " ")


ALL_ASPECTS: Final[frozenset[TableAspect]] = frozenset(TableAspect)
