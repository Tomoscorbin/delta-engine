"""
Compute the actions required to reconcile a table to its desired schema.

`compute_plan` is the single public entry point: given the desired definition and
the currently observed one (or ``None`` when the table is missing), it returns
the `ActionPlan` that closes the gap. The per-dimension diffs (columns,
properties, table comment) are private helpers — they exist only to keep
`compute_plan` readable and have no meaning outside it.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Mapping
from dataclasses import dataclass
import operator

from delta_engine.domain.model import Column, DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    ColumnTypeChange,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    PartitioningChange,
    SetColumnComment,
    SetColumnNullability,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
)


@dataclass(frozen=True, slots=True)
class Diff[D, O]:
    """
    The outcome of matching desired items against observed items by identity.

    Every keyed diff yields four outcomes per item: ``added`` (desired-only),
    ``dropped`` (observed-only), ``changed`` (present on both sides but not
    equal), and noop (present on both and equal). The noop set is the implicit
    complement and is not carried — no consumer reads it.
    """

    added: tuple[D, ...]
    dropped: tuple[O, ...]
    changed: tuple[tuple[D, O], ...]


def diff_by_key[D, O](
    desired: Iterable[D],
    observed: Iterable[O],
    *,
    key: Callable[[D | O], Hashable],
    equals: Callable[[D, O], bool],
) -> Diff[D, O]:
    """
    Match ``desired`` against ``observed`` by ``key`` into add/drop/change buckets.

    ``key`` maps an item (from either side) to its identity. ``equals`` compares
    a matched (desired, observed) pair to decide change vs noop. ``added``
    preserves desired declaration order; ``dropped`` preserves observed order.
    """
    desired_by_key = {key(item): item for item in desired}
    observed_by_key = {key(item): item for item in observed}

    added = tuple(
        item for identity, item in desired_by_key.items() if identity not in observed_by_key
    )
    dropped = tuple(
        item for identity, item in observed_by_key.items() if identity not in desired_by_key
    )
    changed = tuple(
        (desired_item, observed_by_key[identity])
        for identity, desired_item in desired_by_key.items()
        if identity in observed_by_key and not equals(desired_item, observed_by_key[identity])
    )
    return Diff(added=added, dropped=dropped, changed=changed)


def compute_plan(desired: DesiredTable, observed: ObservedTable | None) -> ActionPlan:
    """
    Compute the actions required to reach the desired schema.

    Args:
        desired: Desired table definition.
        observed: Current table definition or ``None`` if the table is missing.

    Returns:
        Action plan describing the necessary changes.

    """
    if observed is None:
        body: tuple[Action, ...] = (CreateTable(desired),)
    else:
        body = (
            _diff_columns(desired.columns, observed.columns)
            + _diff_properties(desired.properties, observed.properties)
            + _diff_table_comment(desired.comment, observed.comment)
            + _diff_partitioning(desired.partitioned_by, observed.partitioned_by)
            + _diff_primary_key(desired, observed)
        )

    # Foreign keys are reconciled the same way whether the table is new or
    # existing: a missing table simply has no observed FKs, so every desired FK
    # is set. Keeping this out of the branch above means there is one FK path,
    # not a hand-rolled "create" variant kept in step with the diff variant.
    return ActionPlan(body + _diff_foreign_keys(desired, observed))


def _diff_columns(desired: tuple[Column, ...], observed: tuple[Column, ...]) -> tuple[Action, ...]:
    """Return the column-level actions to transform `observed` into `desired`."""
    diff = diff_by_key(desired, observed, key=lambda column: column.name, equals=operator.eq)

    add_actions = tuple(AddColumn(column=column) for column in diff.added)
    drop_actions = tuple(DropColumn(column.name) for column in diff.dropped)
    comment_actions = tuple(
        SetColumnComment(desired_column.name, desired_column.comment)
        for desired_column, observed_column in diff.changed
        if desired_column.comment != observed_column.comment
    )
    nullability_actions = tuple(
        SetColumnNullability(column_name=desired_column.name, nullable=desired_column.nullable)
        for desired_column, observed_column in diff.changed
        if desired_column.nullable != observed_column.nullable
    )
    type_change_actions = tuple(
        ColumnTypeChange(
            column_name=desired_column.name,
            from_type=observed_column.data_type,
            to_type=desired_column.data_type,
        )
        for desired_column, observed_column in diff.changed
        if desired_column.data_type != observed_column.data_type
    )
    return add_actions + drop_actions + comment_actions + nullability_actions + type_change_actions


def _diff_properties(
    desired: Mapping[str, str], observed: Mapping[str, str]
) -> tuple[SetProperty, ...]:
    """
    Return the `SetProperty` actions needed to align observed with desired.

    Properties are a declared subset, not a complete desired state: the engine
    only manages keys the user declared. `diff_by_key`'s ``dropped`` bucket is
    deliberately ignored — observed-only keys (e.g. properties Databricks sets
    autonomously) are never unset. Both new keys (``added``) and keys whose
    value differs (``changed``) emit a `SetProperty`.
    """
    diff = diff_by_key(
        desired.items(),
        observed.items(),
        key=lambda item: item[0],
        equals=lambda desired_item, observed_item: desired_item[1] == observed_item[1],
    )
    set_from_added = tuple(SetProperty(name=name, value=value) for name, value in diff.added)
    set_from_changed = tuple(
        SetProperty(name=desired_item[0], value=desired_item[1]) for desired_item, _ in diff.changed
    )
    return set_from_added + set_from_changed


def _diff_partitioning(
    desired: tuple[str, ...], observed: tuple[str, ...]
) -> tuple[PartitioningChange, ...]:
    """Return a PartitioningChange action when the partition specs differ."""
    if desired == observed:
        return ()
    return (PartitioningChange(desired_partitioning=desired, observed_partitioning=observed),)


def _diff_table_comment(desired: str, observed: str) -> tuple[SetTableComment, ...]:
    """Return a comment update action when the desired table comment differs."""
    if desired == observed:
        return ()
    return (SetTableComment(comment=desired),)


def _diff_primary_key(desired: DesiredTable, observed: ObservedTable) -> tuple[Action, ...]:
    """
    Return the primary key actions to align observed with desired.

    Uses set comparison so column order does not trigger spurious changes.
    Declaration order from desired is preserved in SetPrimaryKey.columns.
    """
    desired_primary_key = set(desired.primary_key.columns) if desired.primary_key else set()
    observed_primary_key = set(observed.primary_key.columns) if observed.primary_key else set()

    if desired_primary_key == observed_primary_key:
        return ()

    actions: list[Action] = []

    if observed_primary_key:
        actions.append(DropPrimaryKey())

    if desired_primary_key:
        primary_key_columns = tuple(
            column for column in desired.columns if column.name in desired_primary_key
        )
        constraint_name = desired.primary_key_constraint_name
        assert constraint_name is not None  # guaranteed: desired_primary_key is non-empty
        actions.append(
            SetPrimaryKey(
                columns=primary_key_columns,
                constraint_name=constraint_name,
            )
        )

    return tuple(actions)


def _diff_foreign_keys(desired: DesiredTable, observed: ObservedTable | None) -> tuple[Action, ...]:
    """
    Return the FK actions to align observed with desired.

    ``observed`` is ``None`` when the table does not yet exist. A missing table
    has no observed foreign keys, so every desired FK is set and none is dropped
    — the create case needs no separate code path, it is just a diff against an
    empty set of observed FKs.

    Foreign keys are matched by their content signature (local columns,
    referenced table, and referenced columns), not by constraint name: a
    desired FK whose signature is not observed is set; an observed FK whose
    signature is not desired is dropped; a FK on both sides — even under a
    different constraint name, e.g. one created outside this engine — produces
    no action, so a sync over an unchanged catalog stays idempotent.

    Setting a desired FK uses its resolved name; dropping an observed FK uses
    its catalog-stored name, so the correct constraint is removed. The order
    actions are returned in does not matter — ActionPlan sorts every plan by
    execution phase.
    """
    observed_foreign_keys = observed.foreign_keys if observed is not None else ()
    desired_by_signature = {fk.signature: fk for fk in desired.foreign_keys}
    observed_by_signature = {fk.signature: fk for fk in observed_foreign_keys}

    set_actions = tuple(
        SetForeignKey(
            foreign_key=foreign_key,
            constraint_name=foreign_key.resolve_constraint_name(desired.qualified_name.name),
        )
        for signature, foreign_key in desired_by_signature.items()
        if signature not in observed_by_signature
    )
    drop_actions = tuple(
        DropForeignKey(
            constraint_name=foreign_key.resolve_constraint_name(desired.qualified_name.name)
        )
        for signature, foreign_key in observed_by_signature.items()
        if signature not in desired_by_signature
    )
    return set_actions + drop_actions
