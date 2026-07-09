"""
Atomic differences between desired and observed table state.

Each change is a frozen dataclass recording one fact. Every change carries two
things:

- ``aspect`` — the :class:`TableAspect` the difference belongs to. Validation
  uses this to gate drift in aspects the declaration does not manage.
- ``actions()`` — the imperative actions that reconcile the difference.
  Changes with no in-place remedy (a column type change, a partitioning
  change) return no actions; validation blocks them instead.

Naming conventions:

- Changes are named for what is true relative to the declaration
  (``ColumnAdded``, ``TableTagUnset``); actions in ``actions.py`` are
  imperative commands (``AddColumn``, ``UnsetTableTag``). The two vocabularies
  live in separate modules.
- ``*Changed`` members carry both sides of the difference (``desired_*`` /
  ``observed_*``) as one atomic pair: rules read the direction and report
  from/to values without re-correlating separate changes, and a
  ``__post_init__`` guard makes a no-difference change unrepresentable.
"""

from dataclasses import dataclass
from typing import ClassVar

from delta_engine.domain.model import Column, TableAspect
from delta_engine.domain.model.constraints import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
)
from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.plan.actions import (
    Action,
    AddColumn,
    AlterClustering,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
)


@dataclass(frozen=True, slots=True)
class ColumnAdded:
    """A column present in the declaration but absent from the catalog."""

    column: Column

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def actions(self) -> tuple[Action, ...]:
        """AddColumn for the new column; its tags arrive as ColumnTagSet changes."""
        return (AddColumn(column=self.column),)


@dataclass(frozen=True, slots=True)
class ColumnRemoved:
    """A column present in the catalog but absent from the declaration."""

    column: Column

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def actions(self) -> tuple[Action, ...]:
        """Drop the observed column."""
        return (DropColumn(column_name=self.column.name),)


@dataclass(frozen=True, slots=True)
class ColumnDataTypeChanged:
    """A column whose data type differs — no in-place action is possible."""

    column_name: str
    desired_type: DataType
    observed_type: DataType

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def __post_init__(self) -> None:
        if self.desired_type == self.observed_type:
            raise ValueError(f"ColumnDataTypeChanged carries no difference: {self.desired_type!r}")

    def actions(self) -> tuple[Action, ...]:
        """No actions — type changes require recreation; see ColumnDataTypeChangeNotSupported."""
        return ()


@dataclass(frozen=True, slots=True)
class ColumnNullabilityChanged:
    """A column whose nullability differs."""

    column_name: str
    desired_nullable: bool
    observed_nullable: bool

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def __post_init__(self) -> None:
        if self.desired_nullable == self.observed_nullable:
            raise ValueError(
                f"ColumnNullabilityChanged carries no difference: {self.desired_nullable!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """Set the desired column nullability."""
        return (SetColumnNullability(column_name=self.column_name, nullable=self.desired_nullable),)


@dataclass(frozen=True, slots=True)
class ColumnCommentChanged:
    """A column whose comment differs."""

    column_name: str
    desired_comment: str
    observed_comment: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_COMMENTS

    def __post_init__(self) -> None:
        if self.desired_comment == self.observed_comment:
            raise ValueError(
                f"ColumnCommentChanged carries no difference: {self.desired_comment!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """Set the desired column comment."""
        return (SetColumnComment(column_name=self.column_name, comment=self.desired_comment),)


@dataclass(frozen=True, slots=True)
class ColumnTagSet:
    """A declared column tag absent from the catalog or carrying a different value."""

    column_name: str
    tag_name: str
    tag_value: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_TAGS

    def actions(self) -> tuple[Action, ...]:
        """Set the desired column tag value."""
        return (
            SetColumnTag(column_name=self.column_name, name=self.tag_name, value=self.tag_value),
        )


@dataclass(frozen=True, slots=True)
class ColumnTagUnset:
    """A column tag present in the catalog but absent from the declaration (full-state)."""

    column_name: str
    tag_name: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_TAGS

    def actions(self) -> tuple[Action, ...]:
        """Unset the observed-only column tag."""
        return (UnsetColumnTag(column_name=self.column_name, name=self.tag_name),)


@dataclass(frozen=True, slots=True)
class TableCommentChanged:
    """A table comment that differs from the declaration."""

    desired_comment: str
    observed_comment: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_COMMENT

    def __post_init__(self) -> None:
        if self.desired_comment == self.observed_comment:
            raise ValueError(f"TableCommentChanged carries no difference: {self.desired_comment!r}")

    def actions(self) -> tuple[Action, ...]:
        """Set the desired table comment."""
        return (SetTableComment(comment=self.desired_comment),)


@dataclass(frozen=True, slots=True)
class PropertySet:
    """
    A declared property absent from the catalog or carrying a different value.

    ``observed_value`` is None when the key is absent (first write) and the
    stale catalog value otherwise. An upsert either way — the remedy is the
    same — but both sides travel so validation can judge the transition and
    reports can show was/now.
    """

    name: str
    desired_value: str
    observed_value: str | None

    aspect: ClassVar[TableAspect] = TableAspect.PROPERTIES

    def __post_init__(self) -> None:
        if self.desired_value == self.observed_value:
            raise ValueError(f"PropertySet carries no difference: {self.desired_value!r}")

    def actions(self) -> tuple[Action, ...]:
        """SetProperty with the desired value; observed rides along for rendering."""
        return (
            SetProperty(
                name=self.name, value=self.desired_value, observed_value=self.observed_value
            ),
        )


@dataclass(frozen=True, slots=True)
class PropertyUnset:
    """A property the declaration asserts absent (declared None) but the catalog has."""

    name: str
    observed_value: str

    aspect: ClassVar[TableAspect] = TableAspect.PROPERTIES

    def actions(self) -> tuple[Action, ...]:
        """Unset the observed property."""
        return (UnsetProperty(name=self.name),)


@dataclass(frozen=True, slots=True)
class PropertyUndeclared:
    """
    A managed key present in the catalog but missing from the declaration.

    The engine must not guess: it neither reconciles nor removes the key.
    actions() returns nothing; see PropertyMustBeDeclared.
    """

    name: str
    observed_value: str

    aspect: ClassVar[TableAspect] = TableAspect.PROPERTIES

    def actions(self) -> tuple[Action, ...]:
        """No actions — validation fails the sync instead."""
        return ()


@dataclass(frozen=True, slots=True)
class TableTagSet:
    """A declared table tag absent from the catalog or carrying a different value."""

    name: str
    value: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_TAGS

    def actions(self) -> tuple[Action, ...]:
        """Set the desired table tag value."""
        return (SetTableTag(name=self.name, value=self.value),)


@dataclass(frozen=True, slots=True)
class TableTagUnset:
    """A table tag present in the catalog but absent from the declaration (full-state)."""

    name: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_TAGS

    def actions(self) -> tuple[Action, ...]:
        """Unset the observed-only table tag."""
        return (UnsetTableTag(name=self.name),)


@dataclass(frozen=True, slots=True)
class PartitioningChanged:
    """Partitioning that differs from the declaration — no in-place action is possible."""

    desired_partitioning: tuple[str, ...]
    observed_partitioning: tuple[str, ...]

    aspect: ClassVar[TableAspect] = TableAspect.PARTITIONING

    def __post_init__(self) -> None:
        if self.desired_partitioning == self.observed_partitioning:
            raise ValueError(
                f"PartitioningChanged carries no difference: {self.desired_partitioning!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """No actions — repartitioning requires recreation; see PartitioningChangeNotSupported."""
        return ()


@dataclass(frozen=True, slots=True)
class ClusteringChanged:
    """
    Clustering keys that differ from the declaration — reconciled in place.

    Unlike ``PartitioningChanged``, this emits an action: Delta liquid clustering
    keys can be changed with ``ALTER TABLE ... CLUSTER BY``.
    """

    desired_clustering: tuple[str, ...]
    observed_clustering: tuple[str, ...]

    aspect: ClassVar[TableAspect] = TableAspect.CLUSTERING

    def __post_init__(self) -> None:
        # Clustering-key identity is order-insensitive (Delta: keys may be defined
        # in any order), so compare as sets. Do NOT change this to tuple equality:
        # the catalog can return keys in a different order than declared, and a
        # tuple compare would churn an ALTER CLUSTER BY on every otherwise-clean sync.
        if set(self.desired_clustering) == set(self.observed_clustering):
            raise ValueError(
                f"ClusteringChanged carries no difference: {self.desired_clustering!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """AlterClustering with the desired keys (empty means CLUSTER BY NONE)."""
        return (AlterClustering(columns=self.desired_clustering),)


@dataclass(frozen=True, slots=True)
class PrimaryKeyAdded:
    """A declared primary key absent from the catalog."""

    primary_key: PrimaryKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY

    def actions(self) -> tuple[Action, ...]:
        """Set the desired primary key."""
        return (
            SetPrimaryKey(
                columns=self.primary_key.columns,
                constraint_name=self.primary_key.constraint_name,
            ),
        )


@dataclass(frozen=True, slots=True)
class PrimaryKeyRemoved:
    """
    A primary key present in the catalog but absent from the declaration.

    ``referencing_foreign_keys`` rides along so validation can judge whether
    the key can be dropped; it does not affect ``actions()``. Required, not
    defaulted: a producer must state what references the key, so the
    protection cannot be disabled by omission.
    """

    observed_primary_key: PrimaryKeyConstraint
    referencing_foreign_keys: tuple[ForeignKeyReference, ...]

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY

    def actions(self) -> tuple[Action, ...]:
        """Drop the observed primary key."""
        return (DropPrimaryKey(),)


@dataclass(frozen=True, slots=True)
class PrimaryKeyChanged:
    """
    A primary key whose column set differs from the declaration.

    Both sides travel as one atomic pair so validation can report from/to and
    ``actions()`` can emit Drop then Set. Splitting into separate
    added/removed changes would make an orphaned half representable.

    ``referencing_foreign_keys`` rides along so validation can judge whether
    the key can be dropped; it does not affect ``actions()``. Required, not
    defaulted: a producer must state what references the key, so the
    protection cannot be disabled by omission.
    """

    desired_primary_key: PrimaryKeyConstraint
    observed_primary_key: PrimaryKeyConstraint
    referencing_foreign_keys: tuple[ForeignKeyReference, ...]

    aspect: ClassVar[TableAspect] = TableAspect.PRIMARY_KEY

    def __post_init__(self) -> None:
        if set(self.desired_primary_key.columns) == set(self.observed_primary_key.columns):
            raise ValueError(
                f"PrimaryKeyChanged carries no difference: {self.desired_primary_key!r}"
            )

    def actions(self) -> tuple[Action, ...]:
        """DropPrimaryKey then SetPrimaryKey (ActionPhase orders the pair)."""
        return (
            DropPrimaryKey(),
            SetPrimaryKey(
                columns=self.desired_primary_key.columns,
                constraint_name=self.desired_primary_key.constraint_name,
            ),
        )


@dataclass(frozen=True, slots=True)
class ForeignKeyAdded:
    """A declared foreign key absent from the catalog (by content signature)."""

    constraint: ForeignKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.FOREIGN_KEYS

    def actions(self) -> tuple[Action, ...]:
        """Set the desired foreign key."""
        return (
            SetForeignKey(
                local_columns=self.constraint.local_columns,
                referenced_table=self.constraint.referenced_table,
                referenced_columns=self.constraint.referenced_columns,
                constraint_name=self.constraint.constraint_name,
            ),
        )


@dataclass(frozen=True, slots=True)
class ForeignKeyRemoved:
    """A foreign key present in the catalog but absent from the declaration."""

    constraint: ForeignKeyConstraint

    aspect: ClassVar[TableAspect] = TableAspect.FOREIGN_KEYS

    def actions(self) -> tuple[Action, ...]:
        """DropForeignKey using the catalog-stored constraint name."""
        return (DropForeignKey(constraint_name=self.constraint.constraint_name),)


type Change = (
    ColumnAdded
    | ColumnRemoved
    | ColumnDataTypeChanged
    | ColumnNullabilityChanged
    | ColumnCommentChanged
    | ColumnTagSet
    | ColumnTagUnset
    | TableCommentChanged
    | PropertySet
    | PropertyUnset
    | PropertyUndeclared
    | TableTagSet
    | TableTagUnset
    | PartitioningChanged
    | ClusteringChanged
    | PrimaryKeyAdded
    | PrimaryKeyRemoved
    | PrimaryKeyChanged
    | ForeignKeyAdded
    | ForeignKeyRemoved
)
