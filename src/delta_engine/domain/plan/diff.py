"""
Diff desired and observed table state.

``diff_table`` is the single entry point: given the desired definition and the
observed one (or ``None`` when the table is missing), it returns a
``TableDiff`` — a closed sum of ``TableMissing`` and ``TableDrift``.
``TableDrift`` carries a flat tuple of changes plus the desired table that
produced it — symmetric with ``TableMissing`` — so the diff is self-contained
and ``validate_diff`` needs no other input.

The atomic change vocabulary lives in ``delta_engine.domain.plan.changes``.
This module is responsible for producing those changes, not defining their
lowering semantics.
"""

from collections.abc import Mapping
from dataclasses import dataclass, replace

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
    ActionPlan,
    CreateTable,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnTag,
    SetForeignKey,
    SetTableTag,
)
from delta_engine.domain.plan.changes import (
    Change,
    ClusteringChanged,
    ColumnAdded,
    ColumnCommentChanged,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnRemoved,
    ColumnRenameConflict,
    ColumnRenamed,
    ColumnTagSet,
    ColumnTagUnset,
    ForeignKeyAdded,
    ForeignKeyRemoved,
    PartitioningChanged,
    PrimaryKeyAdded,
    PrimaryKeyChanged,
    PrimaryKeyRemoved,
    PropertySet,
    PropertyUndeclared,
    PropertyUnset,
    TableCommentChanged,
    TableTagSet,
    TableTagUnset,
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
        """Build the action plan, accounting for constraints dropped by a rename."""
        renamed_sources = frozenset(
            change.old_name for change in self.changes if isinstance(change, ColumnRenamed)
        )
        return ActionPlan(
            tuple(
                action
                for change in self.changes
                for action in _planned_actions(
                    change,
                    renamed_sources=renamed_sources,
                    table_name=self.desired.qualified_name,
                )
            )
        )


type TableDiff = TableMissing | TableDrift


def _planned_actions(
    change: Change,
    *,
    renamed_sources: frozenset[str],
    table_name: QualifiedName,
) -> tuple[Action, ...]:
    """Lower a change without pre-dropping constraints the rename drops atomically."""
    actions = change.actions()
    if not renamed_sources:
        return actions

    if isinstance(change, PrimaryKeyRemoved | PrimaryKeyChanged) and not renamed_sources.isdisjoint(
        change.observed_primary_key.columns
    ):
        return tuple(action for action in actions if not isinstance(action, DropPrimaryKey))

    if isinstance(change, ForeignKeyRemoved) and _foreign_key_uses_renamed_column(
        change.constraint,
        renamed_sources=renamed_sources,
        table_name=table_name,
    ):
        return tuple(action for action in actions if not isinstance(action, DropForeignKey))

    return actions


def _foreign_key_uses_renamed_column(
    constraint: ForeignKeyConstraint,
    *,
    renamed_sources: frozenset[str],
    table_name: QualifiedName,
) -> bool:
    """Whether a local or self-referenced FK column is renamed by this table."""
    local_column_renamed = not renamed_sources.isdisjoint(constraint.local_columns)
    self_reference_renamed = (
        constraint.referenced_table == table_name
        and not renamed_sources.isdisjoint(constraint.referenced_columns)
    )
    return local_column_renamed or self_reference_renamed


# ---------------------------------------------------------------------------
# diff_table — change production
# ---------------------------------------------------------------------------


def diff_table(desired: DesiredTable, observed: ObservedTable | None) -> TableDiff:
    """
    Compute the changes separating ``observed`` from ``desired``.

    Returns ``TableMissing`` when the table does not exist, else a
    ``TableDrift`` whose changes each record one atomic difference. An equal
    pair yields an empty drift.

    A rename pre-pass runs first: ``_apply_renames`` projects observed column
    and layout names through the declaration's ``renamed_from`` hints and
    emits ``ColumnRenamed`` facts, so residual drift pairs under the new name.
    A source-and-target-both-observed case becomes an explicit
    ``ColumnRenameConflict`` instead of masquerading as a column removal.

    Each aspect helper takes the two tables and pulls the slice it
    compares. The diff is scope-blind — every aspect is compared regardless
    of ``managed_aspects``, with scope judged in validation — except for
    properties, whose helper gates itself on the declaration's scope (see
    ``_diff_properties`` for why that aspect alone is assertion-like).
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
    changes: tuple[ColumnRenamed | ColumnRenameConflict, ...]


def _apply_renames(desired: DesiredTable, observed: ObservedTable) -> _RenameProjection:
    """
    Project observed column identity through the declaration's rename hints.

    A hint applies only when its source is observed and its target is not. The
    same identity projection covers partitioning and clustering metadata,
    which Delta updates with a mapped column rename. When both names exist,
    emit a conflict and hide the source from ordinary removal diffing.
    """
    renames = {
        column.renamed_from: column.name
        for column in desired.columns
        if column.renamed_from is not None
    }
    observed_names = {column.name for column in observed.columns}
    relabeled = observed.columns
    applied_renames: dict[str, str] = {}
    conflicted_sources: set[str] = set()
    changes: list[ColumnRenamed | ColumnRenameConflict] = []
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
        changes.append(ColumnRenamed(old_name=old_name, new_name=new_name))

    relabeled = tuple(
        column for column in relabeled if column.name not in conflicted_sources
    )
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
) -> list[Change]:
    """Return changes for column additions, removals, type drift, and nullability drift."""
    desired_by_name = {column.name: column for column in desired_columns}
    observed_by_name = {column.name: column for column in observed_columns}
    changes: list[Change] = []

    for name, desired_only_column in desired_by_name.items():
        if name not in observed_by_name:
            changes.append(ColumnAdded(column=desired_only_column))

    for name, observed_only_column in observed_by_name.items():
        if name not in desired_by_name:
            changes.append(ColumnRemoved(column=observed_only_column))

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


def _diff_column_comments(
    desired_columns: tuple[Column, ...], observed_columns: tuple[ObservedColumn, ...]
) -> list[Change]:
    """Comment changes for name-matched column pairs."""
    observed_by_name = {column.name: column for column in observed_columns}
    changes: list[Change] = []
    for column in desired_columns:
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


def _diff_column_tags(
    desired_columns: tuple[Column, ...], observed_columns: tuple[ObservedColumn, ...]
) -> list[Change]:
    """
    Tag changes for every desired column, matched or added (full-state).

    A desired-only column's tags are included: the ADD_COLUMN phase precedes
    SET_COLUMN_TAG, so the column exists by the time its tags are applied.
    """
    observed_by_name = {column.name: column for column in observed_columns}
    changes: list[Change] = []

    for column in desired_columns:
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


def _diff_partitioning(
    desired_partitioning: tuple[str, ...], observed_partitioning: tuple[str, ...]
) -> list[Change]:
    """Return the partitioning change, or nothing when the specs agree."""
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
) -> list[Change]:
    """
    Return the clustering change, or nothing when the key sets agree.

    Identity is set equality, not tuple equality: Delta clustering keys are
    order-insensitive (unlike partitioning, whose order is a directory layout).
    A reordered same-set pair is not a change. The emitted change keeps the
    desired declaration order for rendering CLUSTER BY (...).
    """
    if set(desired_clustering) == set(observed_clustering):
        return []
    return [
        ClusteringChanged(
            desired_clustering=desired_clustering,
            observed_clustering=observed_clustering,
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
