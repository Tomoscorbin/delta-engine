"""
Compare desired and observed table state.

The differ reports every single-table discrepancy — foreign-key existence
included — as either an executable action or an unresolvable difference.
Cross-table judgment (dependency ordering, structural foreign-key verdicts)
lives in ``application/relationships.py``; validation, safety policy,
execution ordering, and backend compilation live elsewhere.
"""

from collections.abc import Iterable, Mapping, Sequence, Set
from dataclasses import dataclass, replace
from typing import Final

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import (
    Array,
    DataType,
    DesiredColumn,
    DesiredForeignKey,
    DesiredTable,
    Map,
    ObservedColumn,
    ObservedForeignKey,
    ObservedTable,
    QualifiedName,
    Struct,
    TableAspect,
    TableFeature,
    TimestampNtz,
    Variant,
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
    EnableTableFeature,
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
    ColumnCaseDrift,
    ColumnRenameConflict,
    PartitioningChanged,
    PropertyUndeclared,
    Unresolvable,
)

_REQUIRED_FEATURE_BY_TYPE: Final[Mapping[type[DataType], TableFeature]] = {
    TimestampNtz: TableFeature.TIMESTAMP_NTZ,
    Variant: TableFeature.VARIANT,
}


@dataclass(frozen=True, slots=True)
class TableCreation:
    """The diff for a table with no catalog counterpart: create everything declared."""

    desired: DesiredTable

    @property
    def target(self) -> QualifiedName:
        """The qualified name of the missing table."""
        return self.desired.qualified_name

    @property
    def actions(self) -> tuple[Action, ...]:
        """Creation actions realizing the complete desired state."""
        return _actions_for_missing_table(self.desired)


@dataclass(frozen=True, slots=True)
class TableDrift:
    """
    Differences separating an observed table from its declaration.

    ``actions`` are remedied differences, each carrying the executable
    operation that closes its gap. ``unresolvable`` are differences no action
    can close; they exist to be judged by validation. Diffing is scope-blind
    except for properties, which are not compared when the declaration does
    not manage them. Validation's eligibility checks decide whether every
    other difference is within the declaration's scope.
    ``desired`` and ``observed`` are the two endpoints the differences
    separate, carried as judging context: the declaration's side (scope and
    declared properties) and the catalog's side (observed facts such
    as the relation kind).
    """

    desired: DesiredTable
    observed: ObservedTable
    actions: ListOrTuple[Action] = ()
    unresolvable: ListOrTuple[Unresolvable] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "actions", tuple(self.actions))
        object.__setattr__(self, "unresolvable", tuple(self.unresolvable))
        _require_same_table(self.desired, self.observed)

    @property
    def target(self) -> QualifiedName:
        return self.desired.qualified_name


type TableDiff = TableCreation | TableDrift


def _require_same_table(desired: DesiredTable, observed: ObservedTable) -> None:
    """Reject endpoints that do not describe the same table."""
    if desired.qualified_name != observed.qualified_name:
        raise ValueError(
            "Cannot compare different tables:"
            f" {desired.qualified_name} != {observed.qualified_name}"
        )


def diff_table(desired: DesiredTable, observed: ObservedTable | None) -> TableDiff:
    """Describe every difference between desired and observed table state."""
    if observed is None:
        return TableCreation(desired=desired)

    return _diff_existing_table(desired, observed)


def _diff_existing_table(desired: DesiredTable, observed: ObservedTable) -> TableDrift:
    """Describe every difference between two states of the same existing table."""
    _require_same_table(desired, observed)

    renames = _resolve_column_renames(desired, observed)

    feature_actions = _diff_required_features(
        desired.columns,
        observed.supported_features,
    )
    column_actions = _diff_columns(desired.columns, renames.columns)
    case_drift = _column_case_drift(desired, observed, renames)
    layout_actions, layout_unresolvable = _diff_layout(desired, renames)
    constraint_actions = (
        *_diff_primary_key(desired, observed),
        *_diff_foreign_keys(desired, observed),
    )
    metadata_actions, metadata_unresolvable = _diff_table_metadata(desired, observed)

    return TableDrift(
        desired=desired,
        observed=observed,
        actions=(
            *feature_actions,
            *renames.actions,
            *column_actions,
            *layout_actions,
            *constraint_actions,
            *metadata_actions,
        ),
        unresolvable=(
            *renames.conflicts,
            *case_drift,
            *metadata_unresolvable,
            *layout_unresolvable,
        ),
    )


def _diff_required_features(
    columns: Iterable[DesiredColumn],
    supported_features: Set[TableFeature],
) -> tuple[EnableTableFeature, ...]:
    """Return upgrades required by the desired column type trees."""
    required_features = {
        feature
        for column in columns
        for data_type in _walk_data_type(column.data_type)
        if (feature := _REQUIRED_FEATURE_BY_TYPE.get(type(data_type))) is not None
    }
    return tuple(
        EnableTableFeature(feature)
        for feature in sorted(required_features - supported_features, key=lambda item: item.value)
    )


def _walk_data_type(data_type: DataType) -> Iterable[DataType]:
    yield data_type
    match data_type:
        case Array(element=element):
            yield from _walk_data_type(element)
        case Map(key=key, value=value):
            yield from _walk_data_type(key)
            yield from _walk_data_type(value)
        case Struct(fields=fields):
            for field in fields:
                yield from _walk_data_type(field.data_type)


def _actions_for_missing_table(desired: DesiredTable) -> tuple[Action, ...]:
    """
    Return every action needed to realize a missing table.

    CREATE TABLE establishes columns, comment, properties, layout, and the
    primary key. Unity Catalog tags and foreign keys need follow-up actions;
    ``SET_FOREIGN_KEY`` phases after ``CREATE_TABLE``, so a self-referential
    key sequences correctly by action phasing alone.
    """
    table_tag_actions = tuple(
        SetTableTag(name=name, desired_value=value, observed_value=None)
        for name, value in desired.tags.items()
    )
    column_tag_actions = tuple(
        SetColumnTag(
            column_name=column.name,
            name=name,
            desired_value=value,
            observed_value=None,
        )
        for column in desired.columns
        for name, value in column.tags.items()
    )
    return (
        CreateTable(desired),
        *table_tag_actions,
        *column_tag_actions,
        *(SetForeignKey(constraint=foreign_key) for foreign_key in desired.foreign_keys),
    )


@dataclass(frozen=True, slots=True)
class _RenameResolution:
    """
    Observed state after applying unambiguous column rename declarations.

    Columns and physical layout use this projected name frame. Constraints
    continue to compare against raw observed state because a physical rename
    drops them.
    """

    columns: ListOrTuple[ObservedColumn]
    partitioned_by: ListOrTuple[str]
    clustered_by: ListOrTuple[str]
    actions: ListOrTuple[RenameColumn]
    conflicts: ListOrTuple[ColumnRenameConflict]

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "partitioned_by", tuple(self.partitioned_by))
        object.__setattr__(self, "clustered_by", tuple(self.clustered_by))
        object.__setattr__(self, "actions", tuple(self.actions))
        object.__setattr__(self, "conflicts", tuple(self.conflicts))


def _resolve_column_renames(desired: DesiredTable, observed: ObservedTable) -> _RenameResolution:
    """Resolve applicable rename hints and project rename-preserved observed state."""
    rename_targets_by_source = {
        column.renamed_from: column for column in desired.columns if column.renamed_from is not None
    }
    observed_by_name = {column.name: column for column in observed.columns}
    new_names_by_old: dict[str, str] = {}
    conflicted_sources: set[str] = set()
    actions: list[RenameColumn] = []
    conflicts: list[ColumnRenameConflict] = []

    for old_name, target in rename_targets_by_source.items():
        observed_column = observed_by_name.get(old_name)
        if observed_column is None:
            continue

        if target.name in observed_by_name:
            conflicted_sources.add(old_name)
            conflicts.append(
                ColumnRenameConflict(old_name=observed_column.name, new_name=target.name)
            )
            continue

        new_names_by_old[old_name] = target.name
        actions.append(RenameColumn(old_name=observed_column.name, new_name=target.name))

    projected_columns = [
        replace(column, name=new_names_by_old[column.name])
        if column.name in new_names_by_old
        else column
        for column in observed.columns
        if column.name not in conflicted_sources
    ]
    return _RenameResolution(
        columns=projected_columns,
        partitioned_by=_project_names(observed.partitioned_by, new_names_by_old),
        clustered_by=_project_names(observed.clustered_by, new_names_by_old),
        actions=actions,
        conflicts=conflicts,
    )


def _project_names(names: Sequence[str], renames: Mapping[str, str]) -> tuple[str, ...]:
    """Project column names through the applied rename mapping."""
    return tuple(renames.get(name, name) for name in names)


@dataclass(frozen=True, slots=True)
class _ColumnAlignment:
    """Desired and rename-projected observed columns classified by name."""

    added: ListOrTuple[DesiredColumn]
    removed: ListOrTuple[ObservedColumn]
    matched: ListOrTuple[tuple[DesiredColumn, ObservedColumn]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "added", tuple(self.added))
        object.__setattr__(self, "removed", tuple(self.removed))
        object.__setattr__(self, "matched", tuple(self.matched))


def _diff_columns(
    desired_columns: Sequence[DesiredColumn],
    observed_columns: Sequence[ObservedColumn],
) -> tuple[Action, ...]:
    """Return every action required to converge the table's columns."""
    alignment = _align_columns(desired_columns, observed_columns)
    actions: list[Action] = []

    for desired in alignment.added:
        actions.extend(_actions_for_added_column(desired))

    for observed in alignment.removed:
        actions.extend(_actions_for_removed_column(observed))

    for desired, observed in alignment.matched:
        actions.extend(_diff_existing_column(desired, observed))

    return tuple(actions)


def _align_columns(
    desired_columns: Sequence[DesiredColumn],
    observed_columns: Sequence[ObservedColumn],
) -> _ColumnAlignment:
    """Classify columns in stable desired and observed order."""
    desired_by_name = {column.name: column for column in desired_columns}
    observed_by_name = {column.name: column for column in observed_columns}

    added = [column for column in desired_columns if column.name not in observed_by_name]
    removed = [column for column in observed_columns if column.name not in desired_by_name]
    matched = [
        (column, observed_by_name[column.name])
        for column in desired_columns
        if column.name in observed_by_name
    ]

    return _ColumnAlignment(
        added=added,
        removed=removed,
        matched=matched,
    )


def _column_case_drift(
    desired: DesiredTable,
    observed: ObservedTable,
    renames: _RenameResolution,
) -> tuple[ColumnCaseDrift, ...]:
    """
    Return every reference to an existing column whose spelling disagrees.

    Matched columns compare against the rename-projected frame, so a renamed
    column wears its declared target spelling and never drifts. A
    ``renamed_from`` hint names a catalog column directly and compares against
    the raw observed frame. New columns and rename targets have no catalog
    counterpart, so nothing compares for them: what a declaration creates, it
    spells freely.
    """
    projected_by_name = {column.name: column for column in renames.columns}
    observed_by_name = {column.name: column for column in observed.columns}
    drift: list[ColumnCaseDrift] = []
    for column in desired.columns:
        matched = projected_by_name.get(column.name)
        if matched is not None and str(column.name) != str(matched.name):
            drift.append(ColumnCaseDrift(declared_name=column.name, observed_name=matched.name))
        source = column.renamed_from
        if source is None:
            continue
        observed_source = observed_by_name.get(source)
        if observed_source is not None and str(source) != str(observed_source.name):
            drift.append(ColumnCaseDrift(declared_name=source, observed_name=observed_source.name))
    return tuple(drift)


def _actions_for_added_column(desired: DesiredColumn) -> tuple[Action, ...]:
    """Add a column, then establish tags not covered by ADD COLUMN."""
    return (
        AddColumn(column=desired),
        *(
            SetColumnTag(
                column_name=desired.name,
                name=name,
                desired_value=value,
                observed_value=None,
            )
            for name, value in desired.tags.items()
        ),
    )


def _actions_for_removed_column(observed: ObservedColumn) -> tuple[Action, ...]:
    """Remove governed tags before dropping their observed column."""
    return (
        *(UnsetColumnTag(column_name=observed.name, name=name) for name in observed.tags),
        DropColumn(column=observed),
    )


def _diff_existing_column(desired: DesiredColumn, observed: ObservedColumn) -> tuple[Action, ...]:
    """Return every field and tag action for a matched column."""
    actions: list[Action] = []

    if desired.data_type != observed.data_type:
        actions.append(
            AlterColumnType(
                column_name=observed.name,
                desired_type=desired.data_type,
                observed_type=observed.data_type,
            )
        )
    if desired.nullable != observed.nullable:
        actions.append(
            SetColumnNullability(
                column_name=observed.name,
                desired_nullable=desired.nullable,
                observed_nullable=observed.nullable,
            )
        )
    if desired.comment != observed.comment:
        actions.append(
            SetColumnComment(
                column_name=observed.name,
                desired_comment=desired.comment,
                observed_comment=observed.comment,
            )
        )

    actions.extend(_diff_column_tags(desired, observed))
    return tuple(actions)


def _diff_column_tags(
    desired: DesiredColumn, observed: ObservedColumn
) -> tuple[SetColumnTag | UnsetColumnTag, ...]:
    """Return full-state tag actions for a matched column."""
    actions: list[SetColumnTag | UnsetColumnTag] = []
    for name, desired_value in desired.tags.items():
        observed_value = observed.tags.get(name)
        if observed_value != desired_value:
            actions.append(
                SetColumnTag(
                    column_name=observed.name,
                    name=name,
                    desired_value=desired_value,
                    observed_value=observed_value,
                )
            )

    actions.extend(
        UnsetColumnTag(column_name=observed.name, name=name)
        for name in observed.tags
        if name not in desired.tags
    )
    return tuple(actions)


def _diff_layout(
    desired: DesiredTable,
    observed: _RenameResolution,
) -> tuple[tuple[AlterClustering, ...], tuple[PartitioningChanged, ...]]:
    """Return resolvable and unresolvable physical-layout differences."""
    actions: tuple[AlterClustering, ...] = ()
    if set(desired.clustered_by) != set(observed.clustered_by):
        actions = (
            AlterClustering(
                desired_clustering=desired.clustered_by,
                observed_clustering=observed.clustered_by,
            ),
        )

    unresolvable: tuple[PartitioningChanged, ...] = ()
    if desired.partitioned_by != observed.partitioned_by:
        unresolvable = (
            PartitioningChanged(
                desired_partitioning=desired.partitioned_by,
                observed_partitioning=observed.partitioned_by,
            ),
        )

    return actions, unresolvable


def _diff_primary_key(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[DropPrimaryKey | SetPrimaryKey, ...]:
    """
    Return primary-key actions; a changed key becomes a drop and a set.

    Desired and observed keys compare directly by relational definition; their
    lifecycle names do not create drift. The comparison runs against raw
    observed names, not the rename-projected frame used by columns and layout:
    renaming a constrained column drops the constraint, so a renamed key must
    surface as an explicit drop and set. A declaration whose column spelling
    disagrees with the catalog is rejected as ``ColumnCaseDrift`` before any
    plan forms.
    """
    desired_key = desired.primary_key
    observed_key = observed.primary_key

    if desired_key is None:
        if observed_key is None:
            return ()
        return (DropPrimaryKey(constraint_name=observed_key.catalog_name),)

    if observed_key is None:
        return (SetPrimaryKey(primary_key=desired_key),)

    if desired_key == observed_key:
        return ()

    return (
        DropPrimaryKey(constraint_name=observed_key.catalog_name),
        SetPrimaryKey(primary_key=desired_key),
    )


def _diff_foreign_keys(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[SetForeignKey | DropForeignKey, ...]:
    """
    Return set/drop actions converging foreign-key definitions.

    Each declaration adopts at most one structurally equal catalog occurrence,
    regardless of its physical name. Sorting observations by exact catalog name
    makes duplicate-definition adoption deterministic. Everything here reads
    the child's own snapshot; whether the referenced table can hold up its end
    is the relationship resolver's judgment, not a difference.
    """
    unmatched_desired: set[DesiredForeignKey | ObservedForeignKey] = set(desired.foreign_keys)
    drops: list[DropForeignKey] = []

    for observed_constraint in sorted(
        observed.foreign_keys,
        key=lambda constraint: (
            constraint.catalog_name.casefold(),
            constraint.catalog_name,
        ),
    ):
        if observed_constraint not in unmatched_desired:
            drops.append(DropForeignKey(constraint=observed_constraint))
            continue
        unmatched_desired.remove(observed_constraint)

    return (
        *drops,
        *(
            SetForeignKey(constraint=constraint)
            for constraint in desired.foreign_keys
            if constraint in unmatched_desired
        ),
    )


def _diff_table_metadata(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[tuple[Action, ...], tuple[PropertyUndeclared, ...]]:
    """Return table comment, property, and tag differences in stable order."""
    property_actions, property_unresolvable = _diff_properties(desired, observed)
    return (
        (
            *_diff_table_comment(desired, observed),
            *property_actions,
            *_diff_table_tags(desired, observed),
        ),
        property_unresolvable,
    )


def _diff_table_comment(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[SetTableComment, ...]:
    """Return the table-comment action, or nothing when comments agree."""
    if desired.comment == observed.comment:
        return ()
    return (
        SetTableComment(
            desired_comment=desired.comment,
            observed_comment=observed.comment,
        ),
    )


def _diff_properties(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[
    tuple[SetProperty | UnsetProperty, ...],
    tuple[PropertyUndeclared, ...],
]:
    """Return all differences implied by exact property declarations."""
    if not desired.scope.manages(TableAspect.PROPERTIES):
        return (), ()

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

    unresolvable = tuple(
        PropertyUndeclared(name=name, observed_value=observed_value)
        for name, observed_value in observed.properties.items()
        if name not in desired.properties
    )
    return tuple(actions), unresolvable


def _diff_table_tags(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[SetTableTag | UnsetTableTag, ...]:
    """Return full-state table-tag actions."""
    actions: list[SetTableTag | UnsetTableTag] = []
    for name, desired_value in desired.tags.items():
        observed_value = observed.tags.get(name)
        if observed_value != desired_value:
            actions.append(
                SetTableTag(
                    name=name,
                    desired_value=desired_value,
                    observed_value=observed_value,
                )
            )
    for name in observed.tags:
        if name not in desired.tags:
            actions.append(UnsetTableTag(name=name))
    return tuple(actions)
