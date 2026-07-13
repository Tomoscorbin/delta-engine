"""Produce executable actions and non-action differences from table state."""

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import ClassVar

from delta_engine.domain.model import (
    Column,
    DesiredTable,
    ForeignKeyConstraint,
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


@dataclass(frozen=True, slots=True)
class ColumnRenameConflict:
    """A declared rename whose source and target are both present."""

    old_name: str
    new_name: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_STRUCTURE

    def __post_init__(self) -> None:
        if self.old_name == self.new_name:
            raise ValueError(f"ColumnRenameConflict carries no difference: {self.old_name!r}")


@dataclass(frozen=True, slots=True)
class PropertyUndeclared:
    """A managed catalog property for which the declaration states no intent."""

    name: str
    observed_value: str

    aspect: ClassVar[TableAspect] = TableAspect.PROPERTIES


@dataclass(frozen=True, slots=True)
class PartitioningChanged:
    """Partitioning drift, which has no supported in-place action."""

    desired_partitioning: tuple[str, ...]
    observed_partitioning: tuple[str, ...]

    aspect: ClassVar[TableAspect] = TableAspect.PARTITIONING

    def __post_init__(self) -> None:
        object.__setattr__(self, "desired_partitioning", tuple(self.desired_partitioning))
        object.__setattr__(self, "observed_partitioning", tuple(self.observed_partitioning))
        if self.desired_partitioning == self.observed_partitioning:
            raise ValueError(
                f"PartitioningChanged carries no difference: {self.desired_partitioning!r}"
            )


type Change = Action | ColumnRenameConflict | PropertyUndeclared | PartitioningChanged


@dataclass(frozen=True, slots=True)
class TableMissing:
    """The table does not exist in the catalog; carries what should exist."""

    desired: DesiredTable


@dataclass(frozen=True, slots=True)
class TableDrift:
    """Actions and other differences separating observed state from its declaration."""

    desired: DesiredTable
    changes: tuple[Change, ...]

    @property
    def managed_changes(self) -> tuple[Change, ...]:
        """Return changes whose aspect the declaration manages."""
        managed = self.desired.managed_aspects
        return tuple(change for change in self.changes if change.aspect in managed)

    @property
    def executable_actions(self) -> tuple[Action, ...]:
        """
        Project diff actions through operation semantics owned by the domain.

        The raw diff retains constraint drops because validation needs their
        observed state. A column rename performs drops for constraints using
        that column atomically, so those redundant actions are omitted only
        from this post-validation projection. This property does not imply
        that the actions are safe; only ``plan_diff`` may construct a plan.
        """
        renamed_sources = {
            action.old_name for action in self.changes if isinstance(action, RenameColumn)
        }
        actions: list[Action] = []
        for change in self.changes:
            if not isinstance(change, Action):
                continue
            if isinstance(change, DropPrimaryKey) and not renamed_sources.isdisjoint(
                change.primary_key.columns
            ):
                continue
            if isinstance(change, DropForeignKey) and _foreign_key_uses_renamed_column(
                change.constraint,
                renamed_sources=renamed_sources,
                table_name=self.desired.qualified_name,
            ):
                continue
            actions.append(change)
        return tuple(actions)


type TableDiff = TableMissing | TableDrift


def _foreign_key_uses_renamed_column(
    constraint: ForeignKeyConstraint,
    *,
    renamed_sources: set[str],
    table_name: QualifiedName,
) -> bool:
    """Whether a local or self-referenced FK column is renamed by this table."""
    local_column_renamed = not renamed_sources.isdisjoint(constraint.local_columns)
    self_reference_renamed = (
        constraint.referenced_table == table_name
        and not renamed_sources.isdisjoint(constraint.referenced_columns)
    )
    return local_column_renamed or self_reference_renamed


def diff_table(desired: DesiredTable, observed: ObservedTable | None) -> TableDiff:
    """
    Compute actions and non-action differences separating observed from desired state.

    A rename pre-pass projects observed columns and layout names through
    ``renamed_from`` hints so residual drift is expressed under the declared
    column name. The diff remains scope-blind except for properties, whose
    declaration has assertion semantics and therefore produces no facts when
    that aspect is unmanaged.
    """
    if observed is None:
        return TableMissing(desired=desired)

    rename_projection = _apply_renames(desired, observed)
    changes: tuple[Change, ...] = (
        *rename_projection.changes,
        *_diff_column_structure(desired.columns, rename_projection.columns),
        *_diff_column_comments(desired.columns, rename_projection.columns),
        *_diff_column_tags(desired.columns, rename_projection.columns),
        *_diff_table_comment(desired, observed),
        *_diff_properties(desired, observed),
        *_diff_table_tags(desired, observed),
        *_diff_partitioning(desired.partitioned_by, rename_projection.partitioned_by),
        *_diff_clustering(desired.clustered_by, rename_projection.clustered_by),
        *_diff_primary_key(desired, observed),
        *_diff_foreign_keys(desired, observed),
    )
    return TableDrift(desired=desired, changes=changes)


@dataclass(frozen=True, slots=True)
class _RenameProjection:
    """Observed names projected through applicable declaration rename hints."""

    columns: tuple[ObservedColumn, ...]
    partitioned_by: tuple[str, ...]
    clustered_by: tuple[str, ...]
    changes: tuple[RenameColumn | ColumnRenameConflict, ...]


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
    changes: list[RenameColumn | ColumnRenameConflict] = []
    for old_name, new_name in renames.items():
        if old_name not in observed_names:
            continue
        if new_name in observed_names:
            conflicted_sources.add(old_name)
            changes.append(ColumnRenameConflict(old_name=old_name, new_name=new_name))
            continue
        relabeled = tuple(
            replace(column, name=new_name) if column.name == old_name else column
            for column in relabeled
        )
        applied_renames[old_name] = new_name
        changes.append(RenameColumn(old_name=old_name, new_name=new_name))

    relabeled = tuple(column for column in relabeled if column.name not in conflicted_sources)
    return _RenameProjection(
        columns=relabeled,
        partitioned_by=_relabel_names(observed.partitioned_by, applied_renames),
        clustered_by=_relabel_names(observed.clustered_by, applied_renames),
        changes=tuple(changes),
    )


def _relabel_names(names: tuple[str, ...], renames: Mapping[str, str]) -> tuple[str, ...]:
    """Project a tuple of column names through applied renames."""
    return tuple(renames.get(name, name) for name in names)


def _diff_column_structure(
    desired_columns: tuple[Column, ...], observed_columns: tuple[ObservedColumn, ...]
) -> list[Action]:
    """Return actions for additions, removals, type drift, and nullability drift."""
    desired_by_name = {column.name: column for column in desired_columns}
    observed_by_name = {column.name: column for column in observed_columns}
    actions: list[Action] = []

    for name, desired_only_column in desired_by_name.items():
        if name not in observed_by_name:
            actions.append(AddColumn(column=desired_only_column))

    for name, observed_only_column in observed_by_name.items():
        if name not in desired_by_name:
            actions.append(DropColumn(column=observed_only_column))

    for name, desired_column in desired_by_name.items():
        observed_column = observed_by_name.get(name)
        if observed_column is None:
            continue
        if desired_column.data_type != observed_column.data_type:
            actions.append(
                AlterColumnType(
                    column_name=name,
                    desired_type=desired_column.data_type,
                    observed_type=observed_column.data_type,
                )
            )
        elif desired_column.nullable != observed_column.nullable:
            actions.append(
                SetColumnNullability(
                    column_name=name,
                    desired_nullable=desired_column.nullable,
                    observed_nullable=observed_column.nullable,
                )
            )
    return actions


def _diff_column_comments(
    desired_columns: tuple[Column, ...], observed_columns: tuple[ObservedColumn, ...]
) -> list[SetColumnComment]:
    """Return comment actions for name-matched column pairs."""
    observed_by_name = {column.name: column for column in observed_columns}
    actions: list[SetColumnComment] = []
    for column in desired_columns:
        observed_column = observed_by_name.get(column.name)
        if observed_column is None or column.comment == observed_column.comment:
            continue
        actions.append(
            SetColumnComment(
                column_name=column.name,
                desired_comment=column.comment,
                observed_comment=observed_column.comment,
            )
        )
    return actions


def _diff_column_tags(
    desired_columns: tuple[Column, ...], observed_columns: tuple[ObservedColumn, ...]
) -> list[SetColumnTag | UnsetColumnTag]:
    """Return full-state tag actions for matched and newly added columns."""
    observed_by_name = {column.name: column for column in observed_columns}
    actions: list[SetColumnTag | UnsetColumnTag] = []
    for column in desired_columns:
        observed_tags: Mapping[str, str] = (
            observed_by_name[column.name].tags if column.name in observed_by_name else {}
        )
        for tag_name, tag_value in column.tags.items():
            if observed_tags.get(tag_name) != tag_value:
                actions.append(
                    SetColumnTag(column_name=column.name, name=tag_name, value=tag_value)
                )
        for tag_name in observed_tags:
            if tag_name not in column.tags:
                actions.append(UnsetColumnTag(column_name=column.name, name=tag_name))
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
) -> list[SetProperty | UnsetProperty | PropertyUndeclared]:
    """Return exact-declaration property actions and undeclared-property differences."""
    if TableAspect.PROPERTIES not in desired.managed_aspects:
        return []

    changes: list[SetProperty | UnsetProperty | PropertyUndeclared] = []
    for name, declared_value in desired.properties.items():
        observed_value = observed.properties.get(name)
        if declared_value is None:
            if observed_value is not None:
                changes.append(UnsetProperty(name=name, observed_value=observed_value))
        elif observed_value != declared_value:
            changes.append(
                SetProperty(
                    name=name,
                    desired_value=declared_value,
                    observed_value=observed_value,
                )
            )

    for name, observed_value in observed.properties.items():
        if name not in desired.properties:
            changes.append(PropertyUndeclared(name=name, observed_value=observed_value))
    return changes


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


def _diff_primary_key(desired: DesiredTable, observed: ObservedTable) -> list[Action]:
    """Return direct primary-key actions; a changed key becomes a drop and a set."""
    desired_key = desired.primary_key
    observed_key = observed.primary_key

    if desired_key is not None and observed_key is None:
        return [SetPrimaryKey(primary_key=desired_key)]

    if desired_key is None and observed_key is not None:
        return [
            DropPrimaryKey(
                primary_key=observed_key,
                referencing_foreign_keys=observed.referencing_foreign_keys,
            )
        ]

    if (
        desired_key is not None
        and observed_key is not None
        and set(desired_key.columns) != set(observed_key.columns)
    ):
        return [
            DropPrimaryKey(
                primary_key=observed_key,
                referencing_foreign_keys=observed.referencing_foreign_keys,
            ),
            SetPrimaryKey(primary_key=desired_key),
        ]

    return []


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
