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
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    UnsupportedChange,
    UnsupportedChangeKind,
)


def _type_name(data_type: object) -> str:
    """Backend-agnostic display name for a domain data type (e.g. 'String')."""
    return type(data_type).__name__


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
            + _diff_primary_key(desired.primary_key, observed.primary_key, desired.columns)
        )

    # Foreign keys are reconciled the same way whether the table is new or
    # existing: a missing table simply has no observed FKs, so every desired FK
    # is set. Keeping this out of the branch above means there is one FK path,
    # not a hand-rolled "create" variant kept in step with the diff variant.
    observed_foreign_keys = observed.foreign_keys if observed is not None else ()
    return ActionPlan(body + _diff_foreign_keys(desired.foreign_keys, observed_foreign_keys))


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
        UnsupportedChange(
            kind=UnsupportedChangeKind.COLUMN_TYPE,
            subject_name=desired_column.name,
            from_repr=_type_name(observed_column.data_type),
            to_repr=_type_name(desired_column.data_type),
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
) -> tuple[UnsupportedChange, ...]:
    """Return an UnsupportedChange action when the partition specs differ."""
    if desired == observed:
        return ()
    return (
        UnsupportedChange(
            kind=UnsupportedChangeKind.PARTITIONING,
            subject_name="",
            from_repr=str(observed),
            to_repr=str(desired),
        ),
    )


def _diff_table_comment(desired: str, observed: str) -> tuple[SetTableComment, ...]:
    """Return a comment update action when the desired table comment differs."""
    if desired == observed:
        return ()
    return (SetTableComment(comment=desired),)


def _diff_primary_key(
    desired_pk: PrimaryKeyConstraint | None,
    observed_pk: PrimaryKeyConstraint | None,
    desired_columns: tuple[Column, ...],
) -> tuple[Action, ...]:
    """
    Return the primary key actions to align observed with desired.

    Uses set comparison so column order does not trigger spurious changes.
    Declaration order from desired is preserved in SetPrimaryKey.columns. The
    desired Column objects are needed so SetPrimaryKey can carry full columns
    (validation checks their nullability); no table name is needed — the
    compiler derives the constraint name.
    """
    desired_columns_in_key = set(desired_pk.columns) if desired_pk else set()
    observed_columns_in_key = set(observed_pk.columns) if observed_pk else set()

    if desired_columns_in_key == observed_columns_in_key:
        return ()

    actions: list[Action] = []
    if observed_columns_in_key:
        actions.append(DropPrimaryKey())
    if desired_columns_in_key:
        primary_key_columns = tuple(
            column for column in desired_columns if column.name in desired_columns_in_key
        )
        actions.append(SetPrimaryKey(columns=primary_key_columns))
    return tuple(actions)


def _diff_foreign_keys(
    desired: tuple[ForeignKeyConstraint, ...],
    observed: tuple[ForeignKeyConstraint, ...],
) -> tuple[Action, ...]:
    """
    Return the FK actions to align observed with desired.

    A missing table has no observed foreign keys (``observed`` is an empty
    tuple), so every desired FK is set and none is dropped — the create case
    needs no separate path.

    Foreign keys are matched by their content signature (local columns,
    referenced table, referenced columns), not by name: because the signature
    is the identity, a matched pair is always equal, so ``changed`` is empty by
    construction. A desired FK whose signature is not observed is set; an
    observed FK whose signature is not desired is dropped; a FK on both sides —
    even under a different constraint name, e.g. one created outside this engine
    — produces no action, so a sync over an unchanged catalog stays idempotent.

    Setting a desired FK carries the FK content; the compiler derives its name.
    Dropping an observed FK uses its catalog-stored name, so the correct
    constraint is removed. Order does not matter — ActionPlan sorts every plan
    by execution phase.
    """
    diff = diff_by_key(
        desired,
        observed,
        key=lambda foreign_key: foreign_key.signature,
        equals=lambda desired_fk, observed_fk: True,
    )
    set_actions = tuple(SetForeignKey(foreign_key=foreign_key) for foreign_key in diff.added)
    drop_actions = tuple(
        DropForeignKey(constraint_name=foreign_key.constraint_name)
        for foreign_key in diff.dropped
        if foreign_key.constraint_name is not None
    )
    return set_actions + drop_actions
