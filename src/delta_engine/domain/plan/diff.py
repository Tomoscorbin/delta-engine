"""
Compare desired and observed table state.

The differ reports every discrepancy as either an executable action or an
unresolvable difference. Validation, safety policy, execution ordering, and
backend compilation live elsewhere.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Final

from delta_engine.domain.model import (
    Array,
    DataType,
    DesiredColumn,
    DesiredTable,
    Map,
    ObservedColumn,
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
class TableMissing:
    """The table does not exist in the catalog; carries what should exist."""

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
class TableInSync:
    """An existing table for which the comparison found no differences."""

    desired: DesiredTable
    observed: ObservedTable

    def __post_init__(self) -> None:
        _require_same_table(self.desired, self.observed)

    @property
    def target(self) -> QualifiedName:
        return self.desired.qualified_name


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
        _require_same_table(self.desired, self.observed)
        if not self.actions and not self.unresolvable:
            raise ValueError("TableDrift must contain at least one difference")

    @property
    def target(self) -> QualifiedName:
        return self.desired.qualified_name


type TableDiff = TableMissing | TableInSync | TableDrift


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
        return TableMissing(desired=desired)

    return _diff_existing_table(desired, observed)


def _diff_existing_table(
    desired: DesiredTable,
    observed: ObservedTable,
) -> TableInSync | TableDrift:
    """Describe every difference between two states of the same existing table."""
    _require_same_table(desired, observed)

    renames = _resolve_column_renames(desired, observed)

    feature_actions = _diff_required_features(
        desired.columns,
        observed.supported_features,
    )
    column_actions = _diff_columns(desired.columns, renames.columns)
    layout_actions, layout_unresolvable = _diff_layout(desired, renames)
    constraint_actions = _diff_constraints(desired, observed)
    metadata_actions, metadata_unresolvable = _diff_table_metadata(desired, observed)

    actions: tuple[Action, ...] = (
        *feature_actions,
        *renames.actions,
        *column_actions,
        *layout_actions,
        *constraint_actions,
        *metadata_actions,
    )
    unresolvable: tuple[Unresolvable, ...] = (
        *renames.conflicts,
        *metadata_unresolvable,
        *layout_unresolvable,
    )
    if not actions and not unresolvable:
        return TableInSync(desired=desired, observed=observed)
    return TableDrift(
        desired=desired,
        observed=observed,
        actions=actions,
        unresolvable=unresolvable,
    )


def _diff_required_features(
    columns: Iterable[DesiredColumn],
    supported_features: frozenset[TableFeature],
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
    primary key. Unity Catalog tags and foreign keys require follow-up actions.
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
    foreign_key_actions = tuple(
        SetForeignKey(constraint=constraint) for constraint in desired.foreign_keys
    )
    return (
        CreateTable(desired),
        *table_tag_actions,
        *column_tag_actions,
        *foreign_key_actions,
    )


@dataclass(frozen=True, slots=True)
class _RenameResolution:
    """
    Observed state after applying unambiguous column rename declarations.

    Columns and physical layout use this projected name frame. Constraints
    continue to compare against raw observed state because a physical rename
    drops them.
    """

    columns: tuple[ObservedColumn, ...]
    partitioned_by: tuple[str, ...]
    clustered_by: tuple[str, ...]
    actions: tuple[RenameColumn, ...]
    conflicts: tuple[ColumnRenameConflict, ...]


def _resolve_column_renames(desired: DesiredTable, observed: ObservedTable) -> _RenameResolution:
    """Resolve applicable rename hints and project rename-preserved observed state."""
    declared_renames = {
        column.renamed_from: column.name
        for column in desired.columns
        if column.renamed_from is not None
    }
    observed_names = {column.name for column in observed.columns}
    applied_renames: dict[str, str] = {}
    conflicted_sources: set[str] = set()
    actions: list[RenameColumn] = []
    conflicts: list[ColumnRenameConflict] = []

    for old_name, new_name in declared_renames.items():
        if old_name not in observed_names:
            continue

        if new_name in observed_names:
            conflicted_sources.add(old_name)
            conflicts.append(ColumnRenameConflict(old_name=old_name, new_name=new_name))
            continue

        applied_renames[old_name] = new_name
        actions.append(RenameColumn(old_name=old_name, new_name=new_name))

    projected_columns = tuple(
        replace(column, name=applied_renames[column.name])
        if column.name in applied_renames
        else column
        for column in observed.columns
        if column.name not in conflicted_sources
    )
    return _RenameResolution(
        columns=projected_columns,
        partitioned_by=_project_names(observed.partitioned_by, applied_renames),
        clustered_by=_project_names(observed.clustered_by, applied_renames),
        actions=tuple(actions),
        conflicts=tuple(conflicts),
    )


def _project_names(names: tuple[str, ...], renames: Mapping[str, str]) -> tuple[str, ...]:
    """Project column names through the applied rename mapping."""
    return tuple(renames.get(name, name) for name in names)


@dataclass(frozen=True, slots=True)
class _ColumnAlignment:
    """Desired and rename-projected observed columns classified by name."""

    added: tuple[DesiredColumn, ...]
    removed: tuple[ObservedColumn, ...]
    matched: tuple[tuple[DesiredColumn, ObservedColumn], ...]


def _diff_columns(
    desired_columns: tuple[DesiredColumn, ...],
    observed_columns: tuple[ObservedColumn, ...],
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
                    column_name=desired.name,
                    name=name,
                    desired_value=desired_value,
                    observed_value=observed_value,
                )
            )

    actions.extend(
        UnsetColumnTag(column_name=desired.name, name=name)
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


def _diff_constraints(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[DropPrimaryKey | SetPrimaryKey | SetForeignKey | DropForeignKey, ...]:
    """
    Return primary- and foreign-key actions against raw observed names.

    Renaming a constrained column drops its constraints, so a renamed key must
    surface as an explicit drop and set rather than compare in the projected
    name frame used by columns and physical layout.
    """
    return (
        *_diff_primary_key(desired, observed),
        *_diff_foreign_keys(desired, observed),
    )


def _diff_primary_key(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[DropPrimaryKey | SetPrimaryKey, ...]:
    """
    Return primary-key actions; a changed key becomes a drop and a set.

    A primary key is identified by its column set, with absence its own
    identity. Dropping one carries its inbound references so validation can
    judge the transition.
    """
    desired_key = desired.primary_key
    observed_key = observed.primary_key

    desired_signature = desired_key.signature if desired_key is not None else None
    observed_signature = observed_key.signature if observed_key is not None else None
    if desired_signature == observed_signature:
        return ()

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
    return tuple(actions)


def _diff_foreign_keys(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[SetForeignKey | DropForeignKey, ...]:
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
    return tuple(actions)


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
    if TableAspect.PROPERTIES not in desired.managed_aspects:
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
