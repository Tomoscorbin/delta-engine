"""Domain models for table snapshots and derivatives."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from types import MappingProxyType
from typing import Final

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.constraints import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
)
from delta_engine.domain.model.qualified_name import QualifiedName


def _validate_key_column_list(kind: str, names: tuple[str, ...], column_names: set[str]) -> None:
    """Rules shared by partition and clustering key lists: lowercase, existing, unique."""
    for name in names:
        if name != name.casefold():
            raise ValueError(f"{kind} column name must be lowercase: {name!r}")

    missing = [name for name in names if name not in column_names]
    if missing:
        raise ValueError(f"{kind} column not found: {', '.join(missing)}")

    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise ValueError(f"Duplicate {kind.casefold()} column: {name}")
        seen.add(name)


class TableAspect(Enum):
    """One independently manageable dimension of a table's state."""

    COLUMN_STRUCTURE = auto()
    COLUMN_COMMENTS = auto()
    COLUMN_TAGS = auto()
    TABLE_COMMENT = auto()
    TABLE_TAGS = auto()
    PROPERTIES = auto()
    PARTITIONING = auto()
    PRIMARY_KEY = auto()
    FOREIGN_KEYS = auto()
    CLUSTERING = auto()

    @property
    def label(self) -> str:
        """Human-readable label (e.g. COLUMN_STRUCTURE -> 'column structure')."""
        return self.name.lower().replace("_", " ")


ALL_ASPECTS: Final[frozenset[TableAspect]] = frozenset(TableAspect)


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """
    Immutable snapshot of a table schema.

    Attributes:
        qualified_name: Fully qualified table name.
        columns: Ordered tuple of ``Column`` definitions.
        comment: Optional table-level comment (empty string when unset).
        tags: Read-only mapping of Unity Catalog tag keys to values.
        partitioned_by: Ordered tuple of partition column names.
        clustered_by: Ordered tuple of liquid clustering column names.
        primary_key: Primary key constraint, or ``None`` when no primary key is defined.

    Properties live on the subclasses, not here: a desired table's mapping
    accepts ``None`` values (absence assertions) while an observed table's
    carries catalog values only — the two types differ.

    """

    qualified_name: QualifiedName
    columns: tuple[Column, ...]
    comment: str = ""
    tags: Mapping[str, str] = field(default_factory=dict)
    partitioned_by: tuple[str, ...] = ()
    clustered_by: tuple[str, ...] = ()
    primary_key: PrimaryKeyConstraint | None = None
    foreign_keys: tuple[ForeignKeyConstraint, ...] = ()

    @property
    def primary_key_columns(self) -> tuple[str, ...]:
        """Primary key column names, or ``()`` when the table has no primary key."""
        return self.primary_key.columns if self.primary_key is not None else ()

    def __post_init__(self) -> None:
        """
        Validate the snapshot's structural invariants.

        Columns must be non-empty and unique; partition and clustering columns
        must be lowercase, must each exist in ``columns``, and must be unique.
        Primary key columns must each exist in ``columns``.
        """
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        object.__setattr__(self, "partitioned_by", tuple(self.partitioned_by))
        object.__setattr__(self, "clustered_by", tuple(self.clustered_by))
        object.__setattr__(self, "foreign_keys", tuple(self.foreign_keys))

        if not self.columns:
            raise ValueError("Table requires at least one column")

        seen_names: set[str] = set()
        for column in self.columns:
            if column.name in seen_names:
                raise ValueError(f"Duplicate column name: {column.name}")
            seen_names.add(column.name)

        _validate_key_column_list("Partition", self.partitioned_by, seen_names)
        _validate_key_column_list("Clustering", self.clustered_by, seen_names)

        if self.primary_key is not None:
            missing_pk = [name for name in self.primary_key.columns if name not in seen_names]
            if missing_pk:
                raise ValueError(f"Primary key column not found in columns: {missing_pk[0]}")

        for foreign_key in self.foreign_keys:
            missing_fk_columns = [
                name for name in foreign_key.local_columns if name not in seen_names
            ]
            if missing_fk_columns:
                raise ValueError(
                    f"Foreign key local column not found in columns: {missing_fk_columns[0]}"
                )

        for tag_key in self.tags:
            if not tag_key.strip():
                raise ValueError(f"Tag key must not be blank: {tag_key!r}")


@dataclass(frozen=True, slots=True)
class DesiredTable(TableSnapshot):
    """
    Desired definition authored by users (target state).

    A desired table's key constraints arrive already named: the constraint name
    is generated by the API layer when a ``DeltaTable`` is lowered. The differ
    and compiler read the name off desired constraints rather than deriving it
    themselves.

    A ``None`` property value asserts the key must be absent from the table.
    """

    properties: Mapping[str, str | None] = field(default_factory=dict)
    managed_aspects: frozenset[TableAspect] = field(default_factory=lambda: ALL_ASPECTS)

    def __post_init__(self) -> None:
        """
        Enforce desired-only invariants.

        No two foreign keys may govern the same set of local columns. Two FKs
        over the same local columns are incoherent, and would generate the same
        constraint name (``{table}_{local_cols}_fk``) and collide at DDL time.
        Checking the column *set* (order-insensitive) also rejects a reordered
        duplicate.

        No two foreign keys may carry the same constraint name. Generated
        names join local columns with underscores, so distinct tuples can
        still collide — ``('a', 'b_c')`` and ``('a_b', 'c')`` both derive
        ``{table}_a_b_c_fk`` — and the second ``ADD CONSTRAINT`` would fail at
        execution with an error that points nowhere near the cause.

        A primary key column must be NOT NULL — a nullable primary key is not a
        well-formed desired schema, independent of any migration. Enforcing it
        here (rather than as a plan-validation rule) keeps the planning layer
        free of column-nullability lookups. Both checks live on DesiredTable,
        not the shared base: an observed table may legitimately carry such a
        layout (a legacy catalog schema) and must stay representable.

        """
        TableSnapshot.__post_init__(self)
        object.__setattr__(self, "properties", MappingProxyType(dict(self.properties)))
        object.__setattr__(self, "managed_aspects", frozenset(self.managed_aspects))

        if not self.managed_aspects:
            raise ValueError(
                "managed_aspects must not be empty: a table that manages no aspect"
                " declares nothing for the engine to do"
            )
        seen: set[frozenset[str]] = set()
        local_columns_by_constraint_name: dict[str, tuple[str, ...]] = {}
        for foreign_key in self.foreign_keys:
            local_column_set = frozenset(foreign_key.local_columns)
            if local_column_set in seen:
                raise ValueError(
                    "Two foreign keys declared over the same local columns:"
                    f" {sorted(local_column_set)}"
                )
            seen.add(local_column_set)
            collided = local_columns_by_constraint_name.get(foreign_key.constraint_name)
            if collided is not None:
                raise ValueError(
                    "Two foreign keys carry the same constraint name"
                    f" '{foreign_key.constraint_name}': local columns {collided}"
                    f" and {foreign_key.local_columns}. Every foreign key on a"
                    " table must have a distinct constraint name."
                )
            local_columns_by_constraint_name[foreign_key.constraint_name] = (
                foreign_key.local_columns
            )

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


@dataclass(frozen=True, slots=True)
class ObservedTable(TableSnapshot):
    """
    Observed definition derived from the catalog (current state).

    ``referencing_foreign_keys`` is the one field that is not about this
    table's own schema: it lists inbound foreign keys owned by other tables,
    read so primary-key changes can be judged for safety. Empty where
    information_schema is unavailable (e.g. plain Spark).
    """

    properties: Mapping[str, str] = field(default_factory=dict)
    referencing_foreign_keys: tuple[ForeignKeyReference, ...] = ()

    def __post_init__(self) -> None:
        TableSnapshot.__post_init__(self)
        object.__setattr__(self, "properties", MappingProxyType(dict(self.properties)))
        object.__setattr__(
            self,
            "referencing_foreign_keys",
            tuple(self.referencing_foreign_keys),
        )
