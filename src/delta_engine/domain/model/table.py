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
        comment: Optional table-level comment (empty string when unset).
        properties: Read-only mapping of table properties.

    """

    qualified_name: QualifiedName
    columns: tuple[Column, ...]
    comment: str = ""
    properties: Mapping[str, str] = field(default_factory=dict)
    partitioned_by: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate columns are non-empty and unique, and partitions exist and are unique."""
        if not self.columns:
            raise ValueError("Table requires at least one column")

        seen_names: set[str] = set()
        for column in self.columns:
            if column.name in seen_names:
                raise ValueError(f"Duplicate column name: {column.name}")
            seen_names.add(column.name)

        if self.partitioned_by:
            for name in self.partitioned_by:
                if name != name.casefold():
                    raise ValueError(f"Partition column name must be lowercase: {name!r}")

            missing = [name for name in self.partitioned_by if name not in seen_names]
            if missing:
                raise ValueError(f"Partition column not found: {missing[0]}")

            seen_partitions: set[str] = set()
            for name in self.partitioned_by:
                if name in seen_partitions:
                    raise ValueError(f"Duplicate partition column: {name}")
                seen_partitions.add(name)


@dataclass(frozen=True, slots=True)
class DesiredTable(TableSnapshot):
    """Desired definition authored by users (target state)."""


@dataclass(frozen=True, slots=True)
class ObservedTable(TableSnapshot):
    """Observed definition derived from the catalog (current state)."""
