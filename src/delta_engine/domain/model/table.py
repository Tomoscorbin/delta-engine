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
        seen_names: set[str] = set()
        for column in self.columns:
            normalized_name = column.name.casefold()
            if normalized_name in seen_names:
                raise ValueError(f"Duplicate column name: {column.name}")
            seen_names.add(normalized_name)

        # Validate that all partition columns exist among defined columns
        if self.partitioned_by:
            missing = [name for name in self.partitioned_by if name.casefold() not in seen_names]
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
