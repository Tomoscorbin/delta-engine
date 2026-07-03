"""
Compute the actions required to reconcile a table to its desired schema.

`compute_plan` is the single public entry point: given the desired definition and
the currently observed one (or ``None`` when the table is missing), it returns
the `ActionPlan` that closes the gap. The per-dimension diffs (column structure,
column comments, properties, table comment, tags, keys) are private helpers —
they exist only to keep `compute_plan` readable and have no meaning outside it.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Mapping
from dataclasses import dataclass

from delta_engine.domain.model import Column, DesiredTable, ObservedTable, TableAspect
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
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    TargetColumnMissing,
    TargetTableMissing,
    UnenforceablePrimaryKey,
    UnsetColumnTag,
    UnsetTableTag,
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
    Compute the actions required to reach the desired state.

    Args:
        desired: Desired table definition.
        observed: Current table definition or ``None`` if the table is missing.

    Returns:
        Action plan describing the necessary changes.

    """
    if observed is None:
        return _plan_for_missing_table(desired)
    return _plan_for_existing_table(desired, observed)


def _plan_for_missing_table(desired: DesiredTable) -> ActionPlan:
    """
    Plan the creation of a table that does not exist yet.

    Creation requires the column structure aspect: CREATE TABLE instantiates
    the full declaration. A definition that does not manage structure cannot
    create the table, so its plan is the descriptive ``TargetTableMissing`` —
    no metadata follow-ups are emitted against a table that will not exist.

    In the create case, tags and foreign keys cannot be declared inline in
    CREATE TABLE, so they are applied as follow-up actions, each gated on its
    own aspect. A missing table has no observed tags or foreign keys, so every
    desired value is set and none is unset.
    """
    managed = desired.managed_aspects
    if TableAspect.COLUMN_STRUCTURE not in managed:
        return ActionPlan((TargetTableMissing(),))

    actions: tuple[Action, ...] = (CreateTable(desired),)
    if TableAspect.TABLE_TAGS in managed:
        actions += _diff_table_tags(desired.tags, {})
    if TableAspect.COLUMN_TAGS in managed:
        actions += _diff_column_tags(desired.columns, ())
    if TableAspect.FOREIGN_KEYS in managed:
        actions += _diff_foreign_keys(desired.foreign_keys, ())
    return ActionPlan(actions)


def _plan_for_existing_table(desired: DesiredTable, observed: ObservedTable) -> ActionPlan:
    """
    Plan the reconciliation of an existing table toward the desired state.

    Every diff dimension runs only when its aspect is managed. Columns are
    matched by name once; structural and metadata diffs read the same match.
    With column structure unmanaged, column metadata reconciles over the
    name-matched subset only — a desired-only column will never come into
    existence, so metadata targeting it is reported as a broken target
    (``_find_broken_targets``) instead of planned.
    """
    managed = desired.managed_aspects
    matched = match_by_key(desired.columns, observed.columns, key=lambda column: column.name)
    structure_is_managed = TableAspect.COLUMN_STRUCTURE in managed
    reconcilable_columns = (
        desired.columns
        if structure_is_managed
        else tuple(desired_column for desired_column, _ in matched.common)
    )

    actions: tuple[Action, ...] = ()
    if structure_is_managed:
        actions += _diff_column_structure(matched)
    if TableAspect.COLUMN_COMMENTS in managed:
        actions += _diff_column_comments(matched.common)
    if TableAspect.PROPERTIES in managed:
        actions += _diff_properties(desired.properties, observed.properties)
    if TableAspect.TABLE_COMMENT in managed:
        actions += _diff_table_comment(desired.comment, observed.comment)
    if TableAspect.PARTITIONING in managed:
        actions += _diff_partitioning(desired.partitioned_by, observed.partitioned_by)
    if TableAspect.PRIMARY_KEY in managed:
        actions += _diff_primary_key(desired.primary_key, observed.primary_key)
    if TableAspect.TABLE_TAGS in managed:
        actions += _diff_table_tags(desired.tags, observed.tags)
    if TableAspect.COLUMN_TAGS in managed:
        actions += _diff_column_tags(reconcilable_columns, observed.columns)
    if TableAspect.FOREIGN_KEYS in managed:
        actions += _diff_foreign_keys(desired.foreign_keys, observed.foreign_keys)

    if not structure_is_managed:
        actions += _find_broken_targets(desired, observed, actions)
    return ActionPlan(actions)


def _diff_column_structure(matched: Matched[Column]) -> tuple[Action, ...]:
    """
    Return the structural column actions: adds, drops, nullability, and types.

    Comments are deliberately excluded — they are metadata, reconciled by
    `_diff_column_comments` over the same match.
    """
    add_actions = tuple(AddColumn(column=column) for column in matched.added)
    drop_actions = tuple(DropColumn(column.name) for column in matched.dropped)
    change_actions: list[Action] = []
    for desired_column, observed_column in matched.common:
        if desired_column.nullable != observed_column.nullable:
            change_actions.append(
                SetColumnNullability(
                    column_name=desired_column.name, nullable=desired_column.nullable
                )
            )
        if desired_column.data_type != observed_column.data_type:
            change_actions.append(
                ColumnTypeChange(
                    column_name=desired_column.name,
                    from_type=observed_column.data_type,
                    to_type=desired_column.data_type,
                )
            )
    return add_actions + drop_actions + tuple(change_actions)


def _diff_column_comments(
    pairs: tuple[tuple[Column, Column], ...],
) -> tuple[Action, ...]:
    """
    Return the comment actions for name-matched column pairs.

    Matching is by name only, so no structural action can arise here: a column
    whose type or nullability drifted still gets its comment reconciled. An
    added column never appears in ``pairs`` — its comment is rendered inline in
    its ADD COLUMN DDL.
    """
    return tuple(
        SetColumnComment(desired_column.name, desired_column.comment)
        for desired_column, observed_column in pairs
        if desired_column.comment != observed_column.comment
    )


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


def _diff_table_tags(desired: Mapping[str, str], observed: Mapping[str, str]) -> tuple[Action, ...]:
    """
    Return the tag actions to align observed tags with desired.

    Tags are full-state: the engine owns the complete tag set. A desired key
    that is absent from observed or carries a different value is set; an
    observed key not present in desired is unset. This is the deliberate
    difference from `_diff_properties` (declared-subset, no unset) and mirrors
    how `_diff_foreign_keys` drops observed-only entries. ActionPlan orders the
    emitted actions by phase and subject, so the return order is not
    load-bearing.
    """
    set_actions: tuple[Action, ...] = tuple(
        SetTableTag(name=name, value=value)
        for name, value in desired.items()
        if observed.get(name) != value
    )
    unset_actions: tuple[Action, ...] = tuple(
        UnsetTableTag(name=name) for name in observed if name not in desired
    )
    return set_actions + unset_actions


def _diff_column_tags(
    desired_columns: tuple[Column, ...],
    observed_columns: tuple[Column, ...],
) -> tuple[Action, ...]:
    """
    Return the column-tag actions to align observed columns with desired.

    Column tags are full-state per column, exactly like `_diff_table_tags` is
    for the table: for each column present in the desired definition, a declared
    tag missing from or differing in the observed column is set, and an observed
    tag not declared is unset. A newly added column (or every column in the
    create case, where `observed_columns` is empty) has no observed tags, so all
    its tags are set and none unset. Columns being dropped are not in
    `desired_columns` and are skipped — dropping the column removes its tags, so
    emitting UnsetColumnTag for them would be redundant (and would target a
    column that no longer exists by the time tag actions run).
    """
    observed_tags_by_column = {column.name: column.tags for column in observed_columns}
    actions: list[Action] = []
    for desired_column in desired_columns:
        observed_column_tags = observed_tags_by_column.get(desired_column.name, {})
        actions.extend(
            SetColumnTag(column_name=desired_column.name, name=name, value=value)
            for name, value in desired_column.tags.items()
            if observed_column_tags.get(name) != value
        )
        actions.extend(
            UnsetColumnTag(column_name=desired_column.name, name=name)
            for name in observed_column_tags
            if name not in desired_column.tags
        )
    return tuple(actions)


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
    constraint, which the API layer generated when the DeltaTable was lowered.
    """
    desired_columns_in_key = set(desired_pk.columns) if desired_pk else set()
    observed_columns_in_key = set(observed_pk.columns) if observed_pk else set()

    if desired_columns_in_key == observed_columns_in_key:
        return ()

    actions: list[Action] = []
    if observed_columns_in_key:
        actions.append(DropPrimaryKey())
    if desired_pk is not None:
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

    Setting a desired FK carries the FK content, including the name the API
    layer generated when the DeltaTable was lowered. Dropping an observed FK
    uses its catalog-stored name, so the correct constraint is removed. Order
    does not matter — ActionPlan sorts every plan by execution phase.
    """
    matched = match_by_key(desired, observed, key=lambda foreign_key: foreign_key.signature)
    set_actions: list[Action] = []
    for foreign_key in matched.added:
        set_actions.append(
            SetForeignKey(
                local_columns=foreign_key.local_columns,
                referenced_table=foreign_key.referenced_table,
                referenced_columns=foreign_key.referenced_columns,
                constraint_name=foreign_key.constraint_name,
            )
        )
    drop_actions = tuple(
        DropForeignKey(constraint_name=foreign_key.constraint_name)
        for foreign_key in matched.dropped
    )
    return tuple(set_actions) + drop_actions


def _find_broken_targets(
    desired: DesiredTable,
    observed: ObservedTable,
    planned: tuple[Action, ...],
) -> tuple[Action, ...]:
    """
    Report every place managed metadata cannot land, as descriptive actions.

    Only meaningful when column structure is unmanaged: with structure managed,
    every declared column either exists or is being added, and nullability is
    tightened before a primary key is set. Emits at most one
    ``TargetColumnMissing`` per column (all reasons on one action, in
    ``TableAspect`` declaration order) plus ``UnenforceablePrimaryKey`` when a
    planned key sits on live-nullable columns.
    """
    observed_column_names = frozenset(column.name for column in observed.columns)
    foreign_key_local_columns = frozenset(
        local_column
        for foreign_key in desired.foreign_keys
        for local_column in foreign_key.local_columns
    )
    # The walrus in the filter both tests and captures the reasons: the yield
    # expression only evaluates when the condition is true, so `reasons` is
    # always bound (and non-empty) where it is read.
    missing_target_actions = tuple(
        TargetColumnMissing(column_name=column.name, reasons=reasons)
        for column in desired.columns
        if column.name not in observed_column_names
        and (reasons := _broken_target_reasons(column, desired, foreign_key_local_columns))
    )
    return missing_target_actions + _unenforceable_primary_key(desired, observed, planned)


def _broken_target_reasons(
    column: Column,
    desired: DesiredTable,
    foreign_key_local_columns: frozenset[str],
) -> tuple[TableAspect, ...]:
    """
    Return the managed aspects whose metadata targets ``column``.

    A column earns a reason only when the aspect is managed AND the column
    actually carries that metadata — a bare stale column is benign drift.
    Reasons follow ``TableAspect`` declaration order so messages are
    deterministic. ``foreign_key_local_columns`` is the pre-computed set of
    all local columns referenced by desired FKs, hoisted by the caller to
    avoid rebuilding it on every invocation.
    """
    managed = desired.managed_aspects
    reasons: list[TableAspect] = []
    if TableAspect.COLUMN_COMMENTS in managed and column.comment:
        reasons.append(TableAspect.COLUMN_COMMENTS)
    if TableAspect.COLUMN_TAGS in managed and column.tags:
        reasons.append(TableAspect.COLUMN_TAGS)
    if TableAspect.PRIMARY_KEY in managed and column.name in desired.primary_key_columns:
        reasons.append(TableAspect.PRIMARY_KEY)
    if TableAspect.FOREIGN_KEYS in managed and column.name in foreign_key_local_columns:
        reasons.append(TableAspect.FOREIGN_KEYS)
    return tuple(reasons)


def _unenforceable_primary_key(
    desired: DesiredTable,
    observed: ObservedTable,
    planned: tuple[Action, ...],
) -> tuple[Action, ...]:
    """
    Flag a planned primary key whose columns are nullable in the live table.

    Fires only when a ``SetPrimaryKey`` is actually planned: a live key that
    already matches the declaration emits no action, so live nullability is
    irrelevant. Key columns missing from the live table entirely are covered
    by ``TargetColumnMissing`` and are not double-reported here.
    """
    if not any(isinstance(action, SetPrimaryKey) for action in planned):
        return ()
    live_nullable_columns = frozenset(
        column.name for column in observed.columns if column.nullable
    )
    nullable_key_columns = tuple(
        name for name in desired.primary_key_columns if name in live_nullable_columns
    )
    if not nullable_key_columns:
        return ()
    return (UnenforceablePrimaryKey(nullable_columns=nullable_key_columns),)
