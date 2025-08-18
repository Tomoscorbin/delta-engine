from __future__ import annotations

from dataclasses import dataclass

from tabula.domain.model.column import Column
from tabula.domain.model.qualified_name import QualifiedName


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """
    Base immutable snapshot of a table schema.
    Case-insensitive names; column order is preserved.
    """

    qualified_name: QualifiedName
    columns: tuple[Column, ...]  # ordered, immutable

    def __post_init__(self) -> None:
        # Must have at least one column
        if not self.columns:
            raise ValueError("CreateTable requires at least one column")

        # duplicate-name detection under case-insensitivity
        seen: set[str] = set()
        for c in self.columns:
            n = c.name.casefold()
            if n in seen:
                raise ValueError(f"Duplicate column name (case-insensitive): {c.name}")
            seen.add(n)

    def __contains__(self, item: str | Column) -> bool:
        if isinstance(item, str):
            n = item.casefold()
            return any(c.name == n for c in self.columns)
        # Column membership by name (case-insensitive)
        n = item.name.casefold()
        return any(c.name == n for c in self.columns)

    def get_column(self, name: str) -> Column | None:
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
    """
    Observed definition derived from the catalog (current state).

    is_empty:
      - True  -> table/view exists and has zero rows
      - False -> table/view exists and has ≥1 row
    """

    is_empty: bool
