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

from delta_engine.domain.model import Column, DesiredTable, ObservedTable
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
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
class Matched[T]:
    """
    The outcome of matching desired items against observed items by identity.

    Partitions purely by key membership: ``added`` (desired-only), ``dropped``
    (observed-only), and ``common`` (present on both sides, paired as
    ``(desired, observed)``). It says nothing about whether a common pair is
    equal — deciding what a match *means* is the caller's concern, not the
    matcher's. ``added`` preserves desired declaration order; ``dropped``
    preserves observed order.
    """

    added: tuple[T, ...]
    dropped: tuple[T, ...]
    common: tuple[tuple[T, T], ...]


def match_by_key[T](
    desired: Iterable[T],
    observed: Iterable[T],
    *,
    key: Callable[[T], Hashable],
) -> Matched[T]:
    """Match ``desired`` against ``observed`` by ``key`` into added/dropped/common."""
    desired_by_key = {key(item): item for item in desired}
    observed_by_key = {key(item): item for item in observed}

    added = tuple(
        item for identity, item in desired_by_key.items() if identity not in observed_by_key
    )
    dropped = tuple(
        item for identity, item in observed_by_key.items() if identity not in desired_by_key
    )
    common = tuple(
        (desired_item, observed_by_key[identity])
        for identity, desired_item in desired_by_key.items()
        if identity in observed_by_key
    )
    return Matched(added=added, dropped=dropped, common=common)


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
            + _diff_primary_key(desired.primary_key, observed.primary_key)
        )

    # Foreign keys are reconciled the same way whether the table is new or
    # existing: a missing table simply has no observed FKs, so every desired FK
    # is set. Keeping this out of the branch above means there is one FK path,
    # not a hand-rolled "create" variant kept in step with the diff variant.
    observed_foreign_keys = observed.foreign_keys if observed is not None else ()
    return ActionPlan(body + _diff_foreign_keys(desired.foreign_keys, observed_foreign_keys))


def _diff_columns(desired: tuple[Column, ...], observed: tuple[Column, ...]) -> tuple[Action, ...]:
    """Return the column-level actions to transform `observed` into `desired`."""
    matched = match_by_key(desired, observed, key=lambda column: column.name)

    add_actions = tuple(AddColumn(column=column) for column in matched.added)
    drop_actions = tuple(DropColumn(column.name) for column in matched.dropped)
    change_actions = tuple(
        action
        for desired_column, observed_column in matched.common
        for action in _reconcile_column(desired_column, observed_column)
    )
    return add_actions + drop_actions + change_actions


def _reconcile_column(desired: Column, observed: Column) -> tuple[Action, ...]:
    """
    Return the actions to align one existing column with its desired form.

    A column present on both sides can differ in comment, nullability, and/or
    data type; each difference is an independent action, and an unchanged column
    yields none. Reconciling all three attributes of a matched pair in one place
    keeps the per-attribute checks from drifting apart and visits each pair once.
    """
    actions: list[Action] = []
    if desired.comment != observed.comment:
        actions.append(SetColumnComment(desired.name, desired.comment))
    if desired.nullable != observed.nullable:
        actions.append(SetColumnNullability(column_name=desired.name, nullable=desired.nullable))
    if desired.data_type != observed.data_type:
        actions.append(
            ColumnTypeChange(
                column_name=desired.name,
                from_type=observed.data_type,
                to_type=desired.data_type,
            )
        )
    return tuple(actions)


def _diff_properties(
    desired: Mapping[str, str], observed: Mapping[str, str]
) -> tuple[SetProperty, ...]:
    """
    Return the `SetProperty` actions needed to align observed with desired.

    Properties are a declared subset, not a complete desired state: the engine
    only manages keys the user declared. A key absent from ``observed`` or
    carrying a different value is set; observed-only keys (e.g. properties
    Databricks sets autonomously) are never unset. ``dict.get`` covers both the
    new-key and changed-value cases in one comparison — properties are a
    mapping, so this direct idiom is clearer than routing them through the
    keyed-collection matcher.
    """
    return tuple(
        SetProperty(name=name, value=value)
        for name, value in desired.items()
        if observed.get(name) != value
    )


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


def _diff_primary_key(
    desired_pk: PrimaryKeyConstraint | None,
    observed_pk: PrimaryKeyConstraint | None,
) -> tuple[Action, ...]:
    """
    Return the primary key actions to align observed with desired.

    Compares the key columns as a set so column order does not trigger a
    spurious change; declaration order from desired is preserved in the emitted
    SetPrimaryKey.columns. The constraint name is read off the desired
    constraint, which was generated when the DesiredTable was built.
    """
    desired_columns_in_key = set(desired_pk.columns) if desired_pk else set()
    observed_columns_in_key = set(observed_pk.columns) if observed_pk else set()

    if desired_columns_in_key == observed_columns_in_key:
        return ()

    actions: list[Action] = []
    if observed_columns_in_key:
        actions.append(DropPrimaryKey())
    if desired_pk is not None:
        assert desired_pk.constraint_name is not None  # generated when DesiredTable was built
        actions.append(
            SetPrimaryKey(columns=desired_pk.columns, constraint_name=desired_pk.constraint_name)
        )
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
    referenced table, referenced columns), not by name. The signature *is* the
    FK's identity, so a matched pair is content-identical and needs no action —
    an FK has no "changed" state, only added or dropped, which is why the
    matcher's ``common`` bucket is unused here. A desired FK whose signature is
    not observed is set; an observed FK whose signature is not desired is
    dropped; a FK on both sides — even under a different constraint name, e.g.
    one created outside this engine — produces no action, so a sync over an
    unchanged catalog stays idempotent.

    Setting a desired FK carries the FK content, including the name generated
    when the DesiredTable was built. Dropping an observed FK uses its
    catalog-stored name, so the correct constraint is removed. Order does not
    matter — ActionPlan sorts every plan by execution phase.
    """
    matched = match_by_key(desired, observed, key=lambda foreign_key: foreign_key.signature)
    set_actions = tuple(SetForeignKey(foreign_key=foreign_key) for foreign_key in matched.added)
    drop_actions = tuple(
        DropForeignKey(constraint_name=foreign_key.constraint_name)
        for foreign_key in matched.dropped
        # An observed FK always carries the catalog name set by the reader; this guard
        # is a type-narrowing safeguard against a reader bug — never false in practice.
        if foreign_key.constraint_name is not None
    )
    return set_actions + drop_actions
