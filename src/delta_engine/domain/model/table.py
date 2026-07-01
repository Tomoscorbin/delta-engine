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
            for foreign_key in self.foreign_keys:
                missing = [col for col in foreign_key.local_columns if col not in seen_names]
                if missing:
                    raise ValueError(f"Foreign key local column not found in columns: {missing[0]}")


@dataclass(frozen=True, slots=True)
class DesiredTable(TableSnapshot):
    """
    Desired definition authored by users (target state).

    A desired table's key constraints carry engine-generated names: the
    constraint name is a property of the desired schema (a pure function of the
    table name and columns), so it is resolved here, once, and then flows
    downstream as data. The differ and compiler read the name off the
    constraint rather than deriving it themselves.
    """

    def __post_init__(self) -> None:
        """
        Enforce desired-only invariants, then generate constraint names.

        No two foreign keys may govern the same set of local columns. Two FKs
        over the same local columns are incoherent, and would generate the same
        constraint name (``{table}_{local_cols}_fk``) and collide at DDL time.
        Checking the column *set* (order-insensitive) also rejects a reordered
        duplicate.

        A primary key column must be NOT NULL — a nullable primary key is not a
        well-formed desired schema, independent of any migration. Enforcing it
        here (rather than as a plan-validation rule) keeps the planning layer
        free of column-nullability lookups. Both checks live on DesiredTable,
        not the shared base: an observed table may legitimately carry such a
        layout (a legacy catalog schema) and must stay representable.

        Names are generated after validation. Each constraint's
        ``with_generated_name`` rejects a name that is already set, so a
        user-supplied constraint name fails loudly here (user-defined names are
        a future feature) rather than being silently ignored downstream.
        """
        TableSnapshot.__post_init__(self)
        seen: set[frozenset[str]] = set()
        for foreign_key in self.foreign_keys:
            local_column_set = frozenset(foreign_key.local_columns)
            if local_column_set in seen:
                raise ValueError(
                    "Two foreign keys declared over the same local columns:"
                    f" {sorted(local_column_set)}"
                )
            seen.add(local_column_set)

        if self.primary_key is not None:
            key_columns = set(self.primary_key.columns)
            nullable_key_columns = [
                column.name
                for column in self.columns
                if column.name in key_columns and column.nullable
            ]
            if nullable_key_columns:
                raise ValueError(
                    "Primary key column must be NOT NULL:"
                    f" {nullable_key_columns[0]}. Set nullable=False on every"
                    " primary key column."
                )

        table_name = self.qualified_name.name
        if self.primary_key is not None:
            object.__setattr__(
                self, "primary_key", self.primary_key.with_generated_name(table_name)
            )
        object.__setattr__(
            self,
            "foreign_keys",
            tuple(fk.with_generated_name(table_name) for fk in self.foreign_keys),
        )


@dataclass(frozen=True, slots=True)
class ObservedTable(TableSnapshot):
    """Observed definition derived from the catalog (current state)."""
