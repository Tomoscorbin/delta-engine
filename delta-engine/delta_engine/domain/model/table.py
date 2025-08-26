"""Domain models for table snapshots and derivatives."""

from __future__ import annotations

from dataclasses import dataclass

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.qualified_name import QualifiedName


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """Immutable snapshot of a table schema.

    Attributes:
        qualified_name: Fully qualified table name.
        columns: Ordered tuple of ``Column`` definitions.

    """

    qualified_name: QualifiedName
    columns: tuple[Column, ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("Table requires at least one column")

        seen: set[str] = set()
        for c in self.columns:
            n = c.name.casefold()
            if n in seen:
                raise ValueError(f"Duplicate column name (case-insensitive): {c.name}")
            seen.add(n)

    def __contains__(self, item: str | Column) -> bool:
        """Return ``True`` if a column with the given name exists."""
        target = item.casefold() if isinstance(item, str) else item.name.casefold()
        return any(col.name.casefold() == target for col in self.columns)

    def get_column(self, name: str) -> Column | None:
        """Return a column by name or ``None`` if not present."""
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
    """Observed definition derived from the catalog (current state).

    Attributes:
        is_empty: ``True`` if the table exists and has zero rows.

    """

    is_empty: bool
