"""Domain models for table snapshots and derivatives."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
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
        partitioned_by: Ordered tuple of partition column names.
        primary_key: Ordered tuple of primary key column names (empty when none).

    """

    qualified_name: QualifiedName
    columns: tuple[Column, ...]
    comment: str = ""
    properties: Mapping[str, str] = field(default_factory=dict)
    partitioned_by: tuple[str, ...] = ()
    primary_key: tuple[str, ...] = ()
    foreign_keys: tuple[ForeignKeyConstraint, ...] = ()

    def __post_init__(self) -> None:
        """
        Validate the snapshot's structural invariants.

        Columns must be non-empty and unique; partition columns must be
        lowercase, must each exist in ``columns``, and must be unique.
        Primary key columns must each exist in ``columns``.
        """
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

        if self.primary_key:
            missing_pk = [name for name in self.primary_key if name not in seen_names]
            if missing_pk:
                raise ValueError(f"Primary key column not found in columns: {missing_pk[0]}")

            seen_pk: set[str] = set()
            for name in self.primary_key:
                if name in seen_pk:
                    raise ValueError(f"Duplicate primary key column: {name}")
                seen_pk.add(name)

        if self.foreign_keys:
            seen_constraint_names: set[str] = set()
            for foreign_key in self.foreign_keys:
                missing = [col for col in foreign_key.local_columns if col not in seen_names]
                if missing:
                    raise ValueError(
                        f"Foreign key local column not found in columns: {missing[0]}"
                    )

                # The differ keys FKs by their resolved constraint name, so two
                # FKs resolving to the same name would silently collapse to one.
                constraint_name = foreign_key.resolve_constraint_name(self.qualified_name.name)
                if constraint_name in seen_constraint_names:
                    raise ValueError(
                        f"Duplicate foreign key constraint name: {constraint_name}"
                    )
                seen_constraint_names.add(constraint_name)


@dataclass(frozen=True, slots=True)
class DesiredTable(TableSnapshot):
    """Desired definition authored by users (target state)."""

    @property
    def primary_key_constraint_name(self) -> str | None:
        """Return the constraint name for this table's primary key, or None if no PK is defined."""
        if not self.primary_key:
            return None
        return f"{self.qualified_name.name}_pk"


@dataclass(frozen=True, slots=True)
class ObservedTable(TableSnapshot):
    """Observed definition derived from the catalog (current state)."""
