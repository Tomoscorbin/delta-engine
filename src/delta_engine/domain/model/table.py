"""Domain models for table snapshots and derivatives."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
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
        primary_key: Primary key constraint, or ``None`` when no primary key is defined.

    """

    qualified_name: QualifiedName
    columns: tuple[Column, ...]
    comment: str = ""
    properties: Mapping[str, str] = field(default_factory=dict)
    partitioned_by: tuple[str, ...] = ()
    primary_key: PrimaryKeyConstraint | None = None
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

        if self.primary_key is not None:
            missing_pk = [name for name in self.primary_key.columns if name not in seen_names]
            if missing_pk:
                raise ValueError(f"Primary key column not found in columns: {missing_pk[0]}")

        if self.foreign_keys:
            seen_explicit_names: set[str] = set()
            seen_unnamed_local_columns: set[tuple[str, ...]] = set()
            for foreign_key in self.foreign_keys:
                missing = [col for col in foreign_key.local_columns if col not in seen_names]
                if missing:
                    raise ValueError(f"Foreign key local column not found in columns: {missing[0]}")

                # Two FKs with the same explicit constraint name would collide in SQL.
                # Two unnamed FKs on the same local columns derive the same name in the
                # SQL adapter, so reject that too.
                if foreign_key.constraint_name is not None:
                    if foreign_key.constraint_name in seen_explicit_names:
                        raise ValueError(
                            f"Duplicate foreign key constraint name: {foreign_key.constraint_name}"
                        )
                    seen_explicit_names.add(foreign_key.constraint_name)
                else:
                    if foreign_key.local_columns in seen_unnamed_local_columns:
                        cols = "_".join(foreign_key.local_columns)
                        derived = f"{self.qualified_name.name}_{cols}_fk"
                        raise ValueError(f"Duplicate foreign key constraint name: {derived}")
                    seen_unnamed_local_columns.add(foreign_key.local_columns)


@dataclass(frozen=True, slots=True)
class DesiredTable(TableSnapshot):
    """Desired definition authored by users (target state)."""


@dataclass(frozen=True, slots=True)
class ObservedTable(TableSnapshot):
    """Observed definition derived from the catalog (current state)."""
