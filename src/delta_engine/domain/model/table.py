"""Domain models for desired and observed table state."""

from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass, field
from enum import Enum, auto
from types import MappingProxyType
from typing import Final

from delta_engine.domain.model.column import DesiredColumn, ObservedColumn
from delta_engine.domain.model.constraints import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
)
from delta_engine.domain.model.frozen import freeze_strings
from delta_engine.domain.model.identifier import Identifier
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.model.table_feature import TableFeature


def _validate_key_column_list(kind: str, names: Sequence[str], column_names: set[str]) -> None:
    """Rules shared by partition and clustering key lists: existing and unique."""
    missing = [name for name in names if str(name) not in column_names]
    if missing:
        raise ValueError(f"{kind} column not found: {', '.join(missing)}")

    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise ValueError(f"Duplicate {kind.lower()} column: {name}")
        seen.add(name)


def _validate_table_structure(
    columns: Sequence[DesiredColumn | ObservedColumn],
    tags: Mapping[str, str],
    partitioned_by: Sequence[str],
    clustered_by: Sequence[str],
    primary_key: PrimaryKeyConstraint | None,
    foreign_keys: Sequence[ForeignKeyConstraint],
) -> None:
    """
    Validate the structural invariants shared by desired and observed tables.

    Columns must be non-empty and unique; partition and clustering columns
    must each exist in ``columns`` and must be unique.
    Primary key and foreign key local columns must each exist in ``columns``.
    Owned references carry the exact column spelling; the public API resolves
    user-supplied casing before constructing a domain table.
    Tag keys must not be blank.
    """
    if not columns:
        raise ValueError("Table requires at least one column")

    seen_names: set[str] = set()
    exact_names: set[str] = set()
    for column in columns:
        if column.name in seen_names:
            raise ValueError(f"Duplicate column name: {column.name}")
        seen_names.add(column.name)
        exact_names.add(str(column.name))

    _validate_key_column_list("Partition", partitioned_by, exact_names)
    _validate_key_column_list("Clustering", clustered_by, exact_names)

    if primary_key is not None:
        missing_pk = [name for name in primary_key.columns if str(name) not in exact_names]
        if missing_pk:
            raise ValueError(f"Primary key column not found in columns: {missing_pk[0]}")

    for foreign_key in foreign_keys:
        missing_fk_columns = [
            name for name in foreign_key.local_columns if str(name) not in exact_names
        ]
        if missing_fk_columns:
            raise ValueError(
                f"Foreign key local column not found in columns: {missing_fk_columns[0]}"
            )

    for tag_key in tags:
        if not tag_key.strip():
            raise ValueError(f"Tag key must not be blank: {tag_key!r}")


class TableAspect(Enum):
    """One independently manageable dimension of a table's state."""

    TABLE_EXISTENCE = auto()
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


class TableKind(Enum):
    """
    The catalog relation kind an observed table resolved to.

    Discovered at read time, never declared. ``TABLE`` is an ordinary managed
    or external Delta table; ``STREAMING_TABLE`` is a pipeline-owned streaming
    table, which takes a distinct ALTER dialect and admits tag changes only
    (enforced by validation's eligibility checks, not here).
    """

    TABLE = auto()
    STREAMING_TABLE = auto()


@dataclass(frozen=True, slots=True)
class DesiredTable:
    """
    Desired definition authored by users (target state).

    Attributes:
        qualified_name: Fully qualified table name.
        columns: Ordered tuple of ``DesiredColumn`` declarations.
        comment: Optional table-level comment (empty string when unset).
        tags: Read-only mapping of Unity Catalog tag keys to values.
        partitioned_by: Ordered tuple of partition column names.
        clustered_by: Ordered tuple of liquid clustering column names.
        primary_key: Primary key constraint, or ``None`` when no primary key is defined.
        foreign_keys: Foreign key constraints owned by this table.
        properties: Table properties; a ``None`` value asserts the key must be
            absent from the table.
        managed_aspects: The table aspects this declaration manages.

    A desired table contains only declared state. Table features implied by
    its column types are derived at the application planning boundary when an
    existing table must be reconciled.

    """

    qualified_name: QualifiedName
    columns: Sequence[DesiredColumn]
    comment: str = ""
    tags: Mapping[str, str] = field(default_factory=dict)
    partitioned_by: Sequence[str] = ()
    clustered_by: Sequence[str] = ()
    primary_key: PrimaryKeyConstraint | None = None
    foreign_keys: Sequence[ForeignKeyConstraint] = ()
    properties: Mapping[str, str | None] = field(default_factory=dict)
    managed_aspects: Set[TableAspect] = field(default_factory=lambda: ALL_ASPECTS)

    @property
    def primary_key_columns(self) -> tuple[str, ...]:
        """Primary key column names, or ``()`` when the table has no primary key."""
        return tuple(self.primary_key.columns) if self.primary_key is not None else ()

    def __post_init__(self) -> None:
        """
        Validate shared table structure, then desired-only invariants.

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
        free of column-nullability lookups. Both checks are desired-only rules,
        not shared structural validation: an observed table may legitimately
        carry such a layout (a legacy catalog schema) and must stay
        representable.

        """
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        partitioned_by = freeze_strings(self.partitioned_by, field_name="partitioned_by")
        clustered_by = freeze_strings(self.clustered_by, field_name="clustered_by")
        object.__setattr__(self, "partitioned_by", tuple(Identifier(n) for n in partitioned_by))
        object.__setattr__(self, "clustered_by", tuple(Identifier(n) for n in clustered_by))
        object.__setattr__(self, "foreign_keys", tuple(self.foreign_keys))
        object.__setattr__(self, "properties", MappingProxyType(dict(self.properties)))
        object.__setattr__(self, "managed_aspects", frozenset(self.managed_aspects))

        _validate_table_structure(
            columns=self.columns,
            tags=self.tags,
            partitioned_by=self.partitioned_by,
            clustered_by=self.clustered_by,
            primary_key=self.primary_key,
            foreign_keys=self.foreign_keys,
        )

        if not self.managed_aspects:
            raise ValueError(
                "managed_aspects must not be empty: a table that manages no aspect"
                " declares nothing for the engine to do"
            )
        seen: set[frozenset[str]] = set()
        local_columns_by_constraint_name: dict[str, Sequence[str]] = {}
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

        declared_names = {column.name for column in self.columns}
        rename_sources: set[str] = set()
        for column in self.columns:
            source = column.renamed_from
            if source is None:
                continue
            if TableAspect.COLUMN_STRUCTURE not in self.managed_aspects:
                raise ValueError(
                    f"Column {column.name!r} declares renamed_from, but this"
                    " declaration does not manage column structure"
                )
            if source in declared_names:
                raise ValueError(
                    f"Column {column.name!r} declares renamed_from {source!r},"
                    f" but {source!r} is still declared. Remove the old column,"
                    " or apply the rename and the reuse of the name in separate"
                    " syncs."
                )
            if source in rename_sources:
                raise ValueError(
                    f"Two columns declare renamed_from {source!r}; a rename source must be unique"
                )
            rename_sources.add(source)


@dataclass(frozen=True, slots=True)
class ObservedTable:
    """
    Observed definition derived from the catalog (current state).

    Attributes:
        qualified_name: Fully qualified table name.
        columns: Ordered tuple of ``ObservedColumn`` definitions.
        comment: Optional table-level comment (empty string when unset).
        tags: Read-only mapping of Unity Catalog tag keys to values.
        partitioned_by: Ordered tuple of partition column names.
        clustered_by: Ordered tuple of liquid clustering column names.
        primary_key: Primary key constraint, or ``None`` when no primary key is defined.
        foreign_keys: Foreign key constraints owned by this table.
        properties: Observed values of the engine-managed property keys only;
            the other keys a table carries are not engine state (values only —
            a catalog has no absence assertions, unlike a desired table's).
        supported_features: Delta table features the table's protocol was
            observed to support, restricted to the features the engine manages;
            the other features a table carries are not engine state.
        referencing_foreign_keys: Inbound foreign keys owned by other tables.
        kind: The relation kind this table resolved to; ``TableKind.TABLE``
            unless the reader observed otherwise.

    ``referencing_foreign_keys`` is the one field that is not about this
    table's own schema: it lists inbound foreign keys owned by other tables,
    read so primary-key changes can be judged for safety. Empty where
    information_schema is unavailable (e.g. plain Spark).

    """

    qualified_name: QualifiedName
    columns: Sequence[ObservedColumn]
    comment: str = ""
    tags: Mapping[str, str] = field(default_factory=dict)
    partitioned_by: Sequence[str] = ()
    clustered_by: Sequence[str] = ()
    primary_key: PrimaryKeyConstraint | None = None
    foreign_keys: Sequence[ForeignKeyConstraint] = ()
    properties: Mapping[str, str] = field(default_factory=dict)
    supported_features: Set[TableFeature] = frozenset()
    referencing_foreign_keys: Sequence[ForeignKeyReference] = ()
    kind: TableKind = TableKind.TABLE

    @property
    def primary_key_columns(self) -> tuple[str, ...]:
        """Primary key column names, or ``()`` when the table has no primary key."""
        return tuple(self.primary_key.columns) if self.primary_key is not None else ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        partitioned_by = freeze_strings(self.partitioned_by, field_name="partitioned_by")
        clustered_by = freeze_strings(self.clustered_by, field_name="clustered_by")
        object.__setattr__(self, "partitioned_by", tuple(Identifier(n) for n in partitioned_by))
        object.__setattr__(self, "clustered_by", tuple(Identifier(n) for n in clustered_by))
        object.__setattr__(self, "foreign_keys", tuple(self.foreign_keys))
        object.__setattr__(self, "properties", MappingProxyType(dict(self.properties)))
        object.__setattr__(self, "supported_features", frozenset(self.supported_features))
        object.__setattr__(self, "referencing_foreign_keys", tuple(self.referencing_foreign_keys))

        _validate_table_structure(
            columns=self.columns,
            tags=self.tags,
            partitioned_by=self.partitioned_by,
            clustered_by=self.clustered_by,
            primary_key=self.primary_key,
            foreign_keys=self.foreign_keys,
        )
