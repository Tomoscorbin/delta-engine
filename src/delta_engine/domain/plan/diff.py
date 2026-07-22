"""
Diff desired and observed table state into actions and unresolvable differences.

``diff_table`` states every difference separating the observed table from
its declaration, deciding nothing about safety or scope. Differences come
in two structural kinds:

- Actions — remedied differences. Each carries the one executable
  operation that closes its gap, plus the desired/observed state
  validation and reporting need.
- Unresolvable — differences no action can close (an ambiguous rename, an
  undeclared managed property, a partitioning change). They exist to be
  judged; the default validation policy rejects each one.
"""

from collections.abc import Mapping
from dataclasses import dataclass, replace

from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    TableAspect,
)
from delta_engine.domain.plan.actions import (
    Action,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    RenameColumn,
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
from delta_engine.domain.plan.unresolvable import (
    ColumnRenameConflict,
    PartitioningChanged,
    PropertyUndeclared,
    Unresolvable,
)


@dataclass(frozen=True, slots=True)
class TableMissing:
    """The table does not exist in the catalog; carries what should exist."""

    desired: DesiredTable

    @property
    def actions(self) -> tuple[Action, ...]:
        """
        Creation actions realizing the complete desired state.

        CREATE TABLE covers columns, comment, properties, layout, and the
        primary key; Unity Catalog tags and foreign keys are applied by
        follow-up actions.
        """
        table_tag_actions = tuple(
            SetTableTag(name=name, value=value) for name, value in self.desired.tags.items()
        )
        column_tag_actions = tuple(
            SetColumnTag(column_name=column.name, name=name, value=value)
            for column in self.desired.columns
            for name, value in column.tags.items()
        )
        foreign_key_actions = tuple(
            SetForeignKey(constraint=constraint) for constraint in self.desired.foreign_keys
        )
        return (
            CreateTable(self.desired),
            *table_tag_actions,
            *column_tag_actions,
            *foreign_key_actions,
        )


@dataclass(frozen=True, slots=True)
class TableDrift:
    """
    Differences separating an observed table from its declaration.

    ``actions`` are remedied differences, each carrying the executable
    operation that closes its gap. ``unresolvable`` are differences no action
    can close; they exist to be judged by validation. Both state every
    difference regardless of scope; deciding which the declaration is
    allowed to make is validation's scope gate, not the diff's concern.
    ``desired`` and ``observed`` are the two endpoints the differences
    separate, carried as judging context: the declaration's side (managed
    aspects, declared properties) and the catalog's side (observed facts such
    as the relation kind).
    """

    desired: DesiredTable
    observed: ObservedTable
    actions: tuple[Action, ...] = ()
    unresolvable: tuple[Unresolvable, ...] = ()

    def __post_init__(self) -> None:
        if self.desired.qualified_name != self.observed.qualified_name:
            raise ValueError(
                "Cannot compare different tables:"
                f" {self.desired.qualified_name} != {self.observed.qualified_name}"
            )

    @property
    def target(self) -> QualifiedName:
        return self.desired.qualified_name


type TableDiff = TableMissing | TableDrift


def diff_table(desired: DesiredTable, observed: ObservedTable | None) -> TableDiff:
    """
    Compute the actions and unresolvable differences separating observed from desired state.

    A rename pre-pass projects observed columns and layout names through
    ``renamed_from`` hints so residual drift is expressed under the declared
    column name. The projection covers exactly the aspects ``RENAME COLUMN``
    preserves (column contents, tags, partitioning, clustering); primary and
    foreign keys, which the rename drops, are deliberately compared under raw
    observed names so their replacement is stated as explicit drop and set
    actions. The diff remains scope-blind except for properties, whose
    declaration has assertion semantics and therefore produces no facts when
    that aspect is unmanaged.
    """
    if observed is None:
        return TableMissing(desired=desired)

    # Column identity is the column name, so aspects normally diff by name.
    # A declared rename breaks that — the column observed as X is the one now
    # declared Y — so renames are resolved first, and each aspect is then diffed
    # in the frame that matches how a rename treats it.
    rename_projection = _apply_renames(desired, observed)
    column_alignment = _align_columns(desired.columns, rename_projection.columns)

    actions: tuple[Action, ...] = (
        *rename_projection.renames,
        # Preserved by a rename: diff under the new names established by the
        # projection, against the declaration's canonical names.
        *_diff_columns(column_alignment),
        *_diff_clustering(desired.clustered_by, rename_projection.clustered_by),
        # Dropped by a rename: diff under raw names so a renamed key column
        # surfaces as an explicit drop-and-set, not silent carry-forward.
        *_diff_primary_key(desired, observed),
        *_diff_foreign_keys(desired, observed),
        # Not column-keyed: renames don't apply.
        *_diff_table_comment(desired, observed),
        *_diff_properties(desired, observed),
        *_diff_table_tags(desired, observed),
    )
    unresolvable: tuple[Unresolvable, ...] = (
        *rename_projection.conflicts,
        *_diff_undeclared_properties(desired, observed),
        *_diff_partitioning(desired.partitioned_by, rename_projection.partitioned_by),
    )
    return TableDrift(
        desired=desired, observed=observed, actions=actions, unresolvable=unresolvable
    )


@dataclass(frozen=True, slots=True)
class _ColumnAlignment:
    """Desired and rename-projected observed columns classified by name."""

    added: tuple[DesiredColumn, ...]
    removed: tuple[ObservedColumn, ...]
    matched: tuple[tuple[DesiredColumn, ObservedColumn], ...]


@dataclass(frozen=True, slots=True)
class _RenameProjection:
    """Observed names projected through applicable declaration rename hints."""

    columns: tuple[ObservedColumn, ...]
    partitioned_by: tuple[str, ...]
    clustered_by: tuple[str, ...]
    renames: tuple[RenameColumn, ...]
    conflicts: tuple[ColumnRenameConflict, ...]


def _align_columns(
    desired_columns: tuple[DesiredColumn, ...],
    observed_columns: tuple[ObservedColumn, ...],
) -> _ColumnAlignment:
    """Classify columns in stable desired and observed order."""
    desired_by_name = {column.name: column for column in desired_columns}
    observed_by_name = {column.name: column for column in observed_columns}

    added = tuple(column for column in desired_columns if column.name not in observed_by_name)
    removed = tuple(column for column in observed_columns if column.name not in desired_by_name)
    matched = tuple(
        (column, observed_by_name[column.name])
        for column in desired_columns
        if column.name in observed_by_name
    )

    return _ColumnAlignment(
        added=added,
        removed=removed,
        matched=matched,
    )


def _apply_renames(desired: DesiredTable, observed: ObservedTable) -> _RenameProjection:
    """Project observed identity through unambiguous declaration rename hints."""
    renames = {
        column.renamed_from: column.name
        for column in desired.columns
        if column.renamed_from is not None
    }
    observed_names = {column.name for column in observed.columns}
    relabeled = observed.columns
    applied_renames: dict[str, str] = {}
    conflicted_sources: set[str] = set()
    rename_actions: list[RenameColumn] = []
    conflicts: list[ColumnRenameConflict] = []
    for old_name, new_name in renames.items():
        if old_name not in observed_names:
            continue
        if new_name in observed_names:
            conflicted_sources.add(old_name)
            conflicts.append(ColumnRenameConflict(old_name=old_name, new_name=new_name))
            continue
        relabeled = tuple(
            replace(column, name=new_name) if column.name == old_name else column
            for column in relabeled
        )
        applied_renames[old_name] = new_name
        rename_actions.append(RenameColumn(old_name=old_name, new_name=new_name))

    # A conflicted source yields no column facts — whether it is surplus or
    # the rename's origin is unknowable, so the conflict is carried as an unresolvable
    # difference instead of a DropColumn.
    relabeled = tuple(column for column in relabeled if column.name not in conflicted_sources)
    return _RenameProjection(
        columns=relabeled,
        partitioned_by=_relabel_names(observed.partitioned_by, applied_renames),
        clustered_by=_relabel_names(observed.clustered_by, applied_renames),
        renames=tuple(rename_actions),
        conflicts=tuple(conflicts),
    )


def _relabel_names(names: tuple[str, ...], renames: Mapping[str, str]) -> tuple[str, ...]:
    """Project a tuple of column names through applied renames."""
    return tuple(renames.get(name, name) for name in names)


def _diff_columns(alignment: _ColumnAlignment) -> list[Action]:
    """Return every action implied by the shared column correspondence."""
    actions: list[Action] = []

    for desired in alignment.added:
        actions.append(AddColumn(column=desired))
        actions.extend(
            SetColumnTag(column_name=desired.name, name=name, value=value)
            for name, value in desired.tags.items()
        )

    for observed in alignment.removed:
        # Governed tags must be removed before Databricks permits the column
        # drop. ActionPlan owns the corresponding execution order.
        actions.extend(
            UnsetColumnTag(column_name=observed.name, name=name) for name in observed.tags
        )
        actions.append(DropColumn(column=observed))

    for desired, observed in alignment.matched:
        if desired.data_type != observed.data_type:
            actions.append(
                AlterColumnType(
                    column_name=desired.name,
                    desired_type=desired.data_type,
                    observed_type=observed.data_type,
                )
            )
        if desired.nullable != observed.nullable:
            actions.append(
                SetColumnNullability(
                    column_name=desired.name,
                    desired_nullable=desired.nullable,
                    observed_nullable=observed.nullable,
                )
            )
        if desired.comment != observed.comment:
            actions.append(
                SetColumnComment(
                    column_name=desired.name,
                    desired_comment=desired.comment,
                    observed_comment=observed.comment,
                )
            )

        for name, value in desired.tags.items():
            if observed.tags.get(name) != value:
                actions.append(SetColumnTag(column_name=desired.name, name=name, value=value))
        actions.extend(
            UnsetColumnTag(column_name=desired.name, name=name)
            for name in observed.tags
            if name not in desired.tags
        )

    return actions


def _diff_table_comment(desired: DesiredTable, observed: ObservedTable) -> list[SetTableComment]:
    """Return the table-comment action, or nothing when comments agree."""
    if desired.comment == observed.comment:
        return []
    return [
        SetTableComment(
            desired_comment=desired.comment,
            observed_comment=observed.comment,
        )
    ]


def _diff_properties(
    desired: DesiredTable, observed: ObservedTable
) -> list[SetProperty | UnsetProperty]:
    """Return exact-declaration property actions for declared keys."""
    # Properties are the sole aspect the differ scopes, and this is fact
    # production, not enforcement (enforcement is validation's scope gate):
    # exact-declaration semantics mean an unmanaged PROPERTIES aspect asserts
    # nothing and so yields no facts. Without this, every managed catalog
    # property the declaration omits would read as unmanaged drift and fail
    # the gate on every restricted sync.
    if TableAspect.PROPERTIES not in desired.managed_aspects:
        return []

    actions: list[SetProperty | UnsetProperty] = []
    for name, declared_value in desired.properties.items():
        observed_value = observed.properties.get(name)
        if declared_value is None and observed_value is not None:
            actions.append(UnsetProperty(name=name, observed_value=observed_value))
        elif declared_value is not None and observed_value != declared_value:
            actions.append(
                SetProperty(
                    name=name,
                    desired_value=declared_value,
                    observed_value=observed_value,
                )
            )
    return actions


def _diff_undeclared_properties(
    desired: DesiredTable, observed: ObservedTable
) -> list[PropertyUndeclared]:
    """Return unresolvable differences for managed catalog keys the declaration omits."""
    # See _diff_properties: exact-declaration semantics, so an unmanaged
    # PROPERTIES aspect produces nothing for the scope gate to reject.
    if TableAspect.PROPERTIES not in desired.managed_aspects:
        return []

    return [
        PropertyUndeclared(name=name, observed_value=observed_value)
        for name, observed_value in observed.properties.items()
        if name not in desired.properties
    ]


def _diff_table_tags(
    desired: DesiredTable, observed: ObservedTable
) -> list[SetTableTag | UnsetTableTag]:
    """Return full-state table-tag actions."""
    actions: list[SetTableTag | UnsetTableTag] = []
    for name, value in desired.tags.items():
        if observed.tags.get(name) != value:
            actions.append(SetTableTag(name=name, value=value))
    for name in observed.tags:
        if name not in desired.tags:
            actions.append(UnsetTableTag(name=name))
    return actions


def _diff_partitioning(
    desired_partitioning: tuple[str, ...], observed_partitioning: tuple[str, ...]
) -> list[PartitioningChanged]:
    """Return a partitioning difference, or nothing when specifications agree."""
    if desired_partitioning == observed_partitioning:
        return []
    return [
        PartitioningChanged(
            desired_partitioning=desired_partitioning,
            observed_partitioning=observed_partitioning,
        )
    ]


def _diff_clustering(
    desired_clustering: tuple[str, ...], observed_clustering: tuple[str, ...]
) -> list[AlterClustering]:
    """Return a clustering action, treating clustering-key identity as a set."""
    if set(desired_clustering) == set(observed_clustering):
        return []
    return [
        AlterClustering(
            desired_clustering=desired_clustering,
            observed_clustering=observed_clustering,
        )
    ]


def _diff_primary_key(
    desired: DesiredTable, observed: ObservedTable
) -> list[DropPrimaryKey | SetPrimaryKey]:
    """
    Return primary-key actions; a changed key becomes a drop and a set.

    A primary key is identified by its column set, with absence its own
    identity, so an unchanged identity yields nothing. Any change drops the
    observed key (carrying the foreign keys that reference it, which the drop
    must account for) and sets the declared one.
    """
    desired_key = desired.primary_key
    observed_key = observed.primary_key

    desired_columns = frozenset(desired_key.columns) if desired_key is not None else None
    observed_columns = frozenset(observed_key.columns) if observed_key is not None else None
    if desired_columns == observed_columns:
        return []

    actions: list[DropPrimaryKey | SetPrimaryKey] = []
    if observed_key is not None:
        actions.append(
            DropPrimaryKey(
                primary_key=observed_key,
                referencing_foreign_keys=observed.referencing_foreign_keys,
            )
        )
    if desired_key is not None:
        actions.append(SetPrimaryKey(primary_key=desired_key))
    return actions


def _diff_foreign_keys(
    desired: DesiredTable, observed: ObservedTable
) -> list[SetForeignKey | DropForeignKey]:
    """Return foreign-key actions matched by content signature."""
    desired_by_signature = {fk.signature: fk for fk in desired.foreign_keys}
    observed_by_signature = {fk.signature: fk for fk in observed.foreign_keys}
    actions: list[SetForeignKey | DropForeignKey] = []
    for signature, constraint in desired_by_signature.items():
        if signature not in observed_by_signature:
            actions.append(SetForeignKey(constraint=constraint))
    for signature, constraint in observed_by_signature.items():
        if signature not in desired_by_signature:
            actions.append(DropForeignKey(constraint=constraint))
    return actions
