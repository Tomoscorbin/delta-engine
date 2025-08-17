from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Optional
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.column import Column

@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """
    Base immutable snapshot of a table schema.
    Case-insensitive names; column order is preserved.
    """
    qualified_name: QualifiedName
    columns: Tuple[Column, ...]  # ordered, immutable

    def __post_init__(self) -> None:
        # duplicate-name detection under case-insensitivity
        seen: set[str] = set()
        for c in self.columns:
            if c.name in seen:
                raise ValueError(f"Duplicate column name (case-insensitive): {c.name}")
            seen.add(c.name)

    def __contains__(self, item: str | Column) -> bool:
        if isinstance(item, str):
            return any(c.name == item.casefold() for c in self.columns)
        return item in self.columns

    def get_column(self, name: str) -> Optional[Column]:
        n = name.casefold()
        for c in self.columns:
            if c.name == n:
                return c
        return None

@dataclass(frozen=True, slots=True)
class DesiredTable(TableSnapshot):
    """Desired definition authored by users (target state)."""

@dataclass(frozen=True, slots=True)
class ObservedTable(TableSnapshot):
    """Observed definition derived from the catalog (current state)."""
