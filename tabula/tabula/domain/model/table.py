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
    columns: tuple[Column, ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("Table requires at least one column")

        # duplicate-name detection under case-insensitivity
        seen: set[str] = set()
        for c in self.columns:
            n = c.name.casefold()
            if n in seen:
                raise ValueError(f"Duplicate column name (case-insensitive): {c.name}")
            seen.add(n)

    def __contains__(self, item: str | Column) -> bool:
        target = item.casefold() if isinstance(item, str) else item.name.casefold()
        return any(col.name.casefold() == target for col in self.columns)

    def get_column(self, name: str) -> Column | None:
        target = name.casefold()
        for col in self.columns:
            if col.name.casefold() == target:
                return col
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
