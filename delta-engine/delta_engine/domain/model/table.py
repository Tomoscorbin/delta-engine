"""Domain models for table snapshots and derivatives."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.qualified_name import QualifiedName


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """
    Immutable snapshot of a table schema.

    Attributes:
        qualified_name: Fully qualified table name.
        columns: Ordered tuple of ``Column`` definitions.

    """

    qualified_name: QualifiedName
    columns: tuple[Column, ...]
    properties: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate non-empty column list and uniqueness by name (casefolded)."""
        if not self.columns:
            raise ValueError("Table requires at least one column")

        seen: set[str] = set()
        for c in self.columns:
            n = c.name.casefold()
            if n in seen:
                raise ValueError(f"Duplicate column name: {c.name}")
            seen.add(n)


@dataclass(frozen=True, slots=True)
class DesiredTable(TableSnapshot):
    """Desired definition authored by users (target state)."""


@dataclass(frozen=True, slots=True)
class ObservedTable(TableSnapshot):
    """
    Observed definition derived from the catalog (current state).

    Attributes:
        is_empty: ``True`` if the table exists and has zero rows.

    """
