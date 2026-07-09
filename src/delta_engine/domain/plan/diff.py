"""
Changes describing how an observed table differs from its desired declaration.

``diff_table`` is the single entry point: given the desired definition and the
observed one (or ``None`` when the table is missing), it returns a
``TableDiff`` — a closed sum of ``TableMissing`` and ``TableDrift``.
``TableDrift`` carries a flat tuple of changes plus the desired table that
produced it — symmetric with ``TableMissing`` — so the diff is
self-contained and ``validate_diff`` needs no other input.

Each change is a frozen dataclass recording one atomic difference. Every
change carries two things:

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

Semantics that differ by aspect:

- Properties are exact-declaration: a declared value is reconciled, a
  declared ``None`` asserts absence, and an observed key without a
  declaration is a blocking change. The observed mapping carries managed
  keys only — the reader adapter filters platform-written keys (e.g.
  ``delta.enableRowTracking``) out of the catalog state. The properties
  diff runs only when the declaration manages ``PROPERTIES``.
- Tags are full-state: an observed-only tag is drift and is unset.
- Nullability drift is suppressed for a column whose type also drifted — the
  column must be recreated first, so a nullability change would be noise.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar

from delta_engine.domain.model import Column, DesiredTable, ObservedTable
from delta_engine.domain.model.constraints import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
)
from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.model.table_aspect import TableAspect
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    AlterClustering,
    CreateTable,
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

# ---------------------------------------------------------------------------
# Changes
# ---------------------------------------------------------------------------


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
        return (SetColumnComment(column_name=self.column_name, comment=self.desired_comment),)


@dataclass(frozen=True, slots=True)
class ColumnTagSet:
    """A declared column tag absent from the catalog or carrying a different value."""

    column_name: str
    tag_name: str
    tag_value: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_TAGS

    def actions(self) -> tuple[Action, ...]:
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
        return (SetTableTag(name=self.name, value=self.value),)


@dataclass(frozen=True, slots=True)
class TableTagUnset:
    """A table tag present in the catalog but absent from the declaration (full-state)."""

    name: str

    aspect: ClassVar[TableAspect] = TableAspect.TABLE_TAGS

    def actions(self) -> tuple[Action, ...]:
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


# ---------------------------------------------------------------------------
# Table-level diff sum
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TableMissing:
    """The table does not exist in the catalog; carries what should exist."""

    desired: DesiredTable

    def plan(self) -> ActionPlan:
        """Build the creation plan: CREATE TABLE plus tag and FK follow-up actions."""
        tag_actions = tuple(
            SetTableTag(name=name, value=value) for name, value in self.desired.tags.items()
        )
        column_tag_actions = tuple(
            SetColumnTag(column_name=column.name, name=name, value=value)
            for column in self.desired.columns
            for name, value in column.tags.items()
        )
        foreign_key_actions = tuple(
            SetForeignKey(
                local_columns=fk.local_columns,
                referenced_table=fk.referenced_table,
                referenced_columns=fk.referenced_columns,
                constraint_name=fk.constraint_name,
            )
            for fk in self.desired.foreign_keys
        )
        return ActionPlan(
            (CreateTable(self.desired), *tag_actions, *column_tag_actions, *foreign_key_actions)
        )


@dataclass(frozen=True, slots=True)
class TableDrift:
    """
    Flat sequence of changes separating an observed table from its declaration.

    Carries the desired table itself — symmetric with ``TableMissing`` — so
    the diff is self-contained: validation reads the declaration's scope and
    properties from the drift with no second argument. The natural zero is
    an empty changes tuple (no drift).
    """

    desired: DesiredTable
    changes: tuple[Change, ...]

    @property
    def managed_changes(self) -> tuple[Change, ...]:
        """
        Changes whose aspect the declaration manages.

        Safety rules judge these — a change in an unmanaged aspect is a scope
        violation reported once (see the validator's scope check), not input
        for the rules, so filtering here keeps unmanaged drift from also
        tripping a safety rule.
        """
        managed = self.desired.managed_aspects
        return tuple(change for change in self.changes if change.aspect in managed)

    def plan(self) -> ActionPlan:
        """Build the action plan by collecting actions from every change."""
        return ActionPlan(tuple(action for change in self.changes for action in change.actions()))


type TableDiff = TableMissing | TableDrift


# ---------------------------------------------------------------------------
# diff_table — change production
# ---------------------------------------------------------------------------


def diff_table(desired: DesiredTable, observed: ObservedTable | None) -> TableDiff:
    """
    Compute the changes separating ``observed`` from ``desired``.

    Returns ``TableMissing`` when the table does not exist, else a
    ``TableDrift`` whose changes each record one atomic difference. An equal
    pair yields an empty drift.

    Each aspect helper takes the two tables and pulls the slice it
    compares. The diff is scope-blind — every aspect is compared regardless
    of ``managed_aspects``, with scope judged in validation — except for
    properties, whose helper gates itself on the declaration's scope (see
    ``_diff_properties`` for why that aspect alone is assertion-like).
    """
    if observed is None:
        return TableMissing(desired=desired)

    changes: tuple[Change, ...] = (
        *_diff_column_structure(desired, observed),
        *_diff_column_comments(desired, observed),
        *_diff_column_tags(desired, observed),
        *_diff_table_comment(desired, observed),
        *_diff_properties(desired, observed),
        *_diff_table_tags(desired, observed),
        *_diff_partitioning(desired, observed),
        *_diff_clustering(desired, observed),
        *_diff_primary_key(desired, observed),
        *_diff_foreign_keys(desired, observed),
    )
    return TableDrift(desired=desired, changes=changes)


def _diff_column_structure(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """Return changes for column additions, removals, type drift, and nullability drift."""
    desired_by_name = {column.name: column for column in desired.columns}
    observed_by_name = {column.name: column for column in observed.columns}
    changes: list[Change] = []

    for name, column in desired_by_name.items():
        if name not in observed_by_name:
            changes.append(ColumnAdded(column=column))

    for name, column in observed_by_name.items():
        if name not in desired_by_name:
            changes.append(ColumnRemoved(column=column))

    for name, desired_column in desired_by_name.items():
        observed_column = observed_by_name.get(name)
        if observed_column is None:
            continue
        if desired_column.data_type != observed_column.data_type:
            changes.append(
                ColumnDataTypeChanged(
                    column_name=name,
                    desired_type=desired_column.data_type,
                    observed_type=observed_column.data_type,
                )
            )
        elif desired_column.nullable != observed_column.nullable:
            changes.append(
                ColumnNullabilityChanged(
                    column_name=name,
                    desired_nullable=desired_column.nullable,
                    observed_nullable=observed_column.nullable,
                )
            )
    return changes


def _diff_column_comments(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """Comment changes for name-matched column pairs."""
    observed_by_name = {column.name: column for column in observed.columns}
    changes: list[Change] = []
    for column in desired.columns:
        if column.name not in observed_by_name:
            continue
        observed_column = observed_by_name[column.name]
        if column.comment != observed_column.comment:
            changes.append(
                ColumnCommentChanged(
                    column_name=column.name,
                    desired_comment=column.comment,
                    observed_comment=observed_column.comment,
                )
            )
    return changes


def _diff_column_tags(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """
    Tag changes for every desired column, matched or added (full-state).

    A desired-only column's tags are included: the ADD_COLUMN phase precedes
    SET_COLUMN_TAG, so the column exists by the time its tags are applied.
    """
    observed_by_name = {column.name: column for column in observed.columns}
    changes: list[Change] = []

    for column in desired.columns:
        observed_tags: Mapping[str, str] = (
            observed_by_name[column.name].tags if column.name in observed_by_name else {}
        )

        for tag_name, tag_value in column.tags.items():
            if observed_tags.get(tag_name) != tag_value:
                changes.append(
                    ColumnTagSet(column_name=column.name, tag_name=tag_name, tag_value=tag_value)
                )

        for tag_name in observed_tags:
            if tag_name not in column.tags:
                changes.append(ColumnTagUnset(column_name=column.name, tag_name=tag_name))

    return changes


def _diff_table_comment(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """Return the table comment change, or nothing when the comments agree."""
    if desired.comment == observed.comment:
        return []
    return [TableCommentChanged(desired_comment=desired.comment, observed_comment=observed.comment)]


def _diff_properties(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """
    Property changes under exact-declaration semantics.

    The declaration is the complete list of managed keys: a declared value
    is reconciled, a declared None asserts absence (unset when present),
    and an observed key without a declaration is a blocking change — the
    engine must not guess. The observed mapping contains managed keys only:
    the reader adapter filters platform-written keys out of the catalog
    state before the domain sees them.

    Unlike every other aspect, this helper gates itself on scope: an empty
    property mapping is an assertion (every observed key becomes a blocking
    change), not a neutral absence of facts — so a declaration that does
    not manage ``PROPERTIES`` must make no assertion at all, and the diff
    returns nothing rather than facts for validation to judge.
    """
    if TableAspect.PROPERTIES not in desired.managed_aspects:
        return []

    changes: list[Change] = []

    for name, declared_value in desired.properties.items():
        observed_value = observed.properties.get(name)
        if declared_value is None:
            if observed_value is not None:
                changes.append(PropertyUnset(name=name, observed_value=observed_value))
        elif observed_value != declared_value:
            changes.append(
                PropertySet(name=name, desired_value=declared_value, observed_value=observed_value)
            )

    for name, observed_value in observed.properties.items():
        if name not in desired.properties:
            changes.append(PropertyUndeclared(name=name, observed_value=observed_value))

    return changes


def _diff_table_tags(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """Tag changes under full-state semantics: observed-only tags are drift and are unset."""
    changes: list[Change] = []

    for name, value in desired.tags.items():
        if observed.tags.get(name) != value:
            changes.append(TableTagSet(name=name, value=value))

    for name in observed.tags:
        if name not in desired.tags:
            changes.append(TableTagUnset(name=name))

    return changes


def _diff_partitioning(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """Return the partitioning change, or nothing when the specs agree."""
    if desired.partitioned_by == observed.partitioned_by:
        return []
    return [
        PartitioningChanged(
            desired_partitioning=desired.partitioned_by,
            observed_partitioning=observed.partitioned_by,
        )
    ]


def _diff_clustering(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """
    Return the clustering change, or nothing when the key sets agree.

    Identity is set equality, not tuple equality: Delta clustering keys are
    order-insensitive (unlike partitioning, whose order is a directory layout).
    A reordered same-set pair is not a change. The emitted change keeps the
    desired declaration order for rendering CLUSTER BY (...).
    """
    if set(desired.clustered_by) == set(observed.clustered_by):
        return []
    return [
        ClusteringChanged(
            desired_clustering=desired.clustered_by,
            observed_clustering=observed.clustered_by,
        )
    ]


def _diff_primary_key(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """
    Return the primary key change, or nothing when the keys agree.

    Identity is column-set equality: order and constraint name do not make
    two keys different.
    """
    desired_key = desired.primary_key
    observed_key = observed.primary_key

    if desired_key is not None and observed_key is None:
        return [PrimaryKeyAdded(primary_key=desired_key)]

    if desired_key is None and observed_key is not None:
        return [
            PrimaryKeyRemoved(
                observed_primary_key=observed_key,
                referencing_foreign_keys=observed.referencing_foreign_keys,
            )
        ]

    if (
        desired_key is not None
        and observed_key is not None
        and set(desired_key.columns) != set(observed_key.columns)
    ):
        return [
            PrimaryKeyChanged(
                desired_primary_key=desired_key,
                observed_primary_key=observed_key,
                referencing_foreign_keys=observed.referencing_foreign_keys,
            )
        ]

    return []


def _diff_foreign_keys(desired: DesiredTable, observed: ObservedTable) -> list[Change]:
    """
    Foreign key changes, matched by content signature.

    Identity is content (local columns, referenced table, referenced columns):
    an FK present on both sides under different constraint names produces
    nothing, so a sync over an unchanged catalog stays idempotent.
    """
    desired_by_signature = {fk.signature: fk for fk in desired.foreign_keys}
    observed_by_signature = {fk.signature: fk for fk in observed.foreign_keys}
    changes: list[Change] = []

    for signature, fk in desired_by_signature.items():
        if signature not in observed_by_signature:
            changes.append(ForeignKeyAdded(constraint=fk))

    for signature, fk in observed_by_signature.items():
        if signature not in desired_by_signature:
            changes.append(ForeignKeyRemoved(constraint=fk))

    return changes
