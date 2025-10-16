"""Domain models for table snapshots and derivatives."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.qualified_name import QualifiedName


class TableFormat(StrEnum):
    """Supported table formats."""

    DELTA = "delta"


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
        """Validate non-empty column list and uniqueness by name (casefolded)."""
        if not self.columns:
            raise ValueError("Table requires at least one column")

        # validate that there are no duplicate columns
        seen: set[str] = set()
        for c in self.columns:
            n = c.name.casefold()
            if n in seen:
                raise ValueError(f"Duplicate column name: {c.name}")
            seen.add(n)

        # Validate that all partition columns exist among defined columns
        if self.partitioned_by:
            missing = [p for p in self.partitioned_by if p.casefold() not in seen]
            if missing:
                raise ValueError(f"Partition column not found: {missing[0]}")


@dataclass(frozen=True, slots=True)
class DesiredTable(TableSnapshot):
    """Desired definition authored by users (target state)."""

    format: TableFormat = field(kw_only=True)

    # We only carry `format` on DesiredTable for creation.
    # We currently assume table format is *immutable* after creation and do not
    # detect or plan format changes.


@dataclass(frozen=True, slots=True)
class ObservedTable(TableSnapshot):
    """Observed definition derived from the catalog (current state)."""
