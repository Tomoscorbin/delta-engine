"""
Compute the actions required to reconcile a table to its desired schema.

`compute_plan` is the single public entry point: given the desired definition and
the currently observed one (or ``None`` when the table is missing), it returns
the `ActionPlan` that closes the gap. The per-dimension diffs (columns,
properties, table comment) are private helpers — they exist only to keep
`compute_plan` readable and have no meaning outside it.
"""

from __future__ import annotations

from collections.abc import Mapping

from delta_engine.domain.model import Column, DesiredTable, ObservedTable
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
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
        foreign_key_actions = tuple(
            SetForeignKey(
                foreign_key=fk,
                constraint_name=desired.resolve_foreign_key_constraint_name(fk),
            )
            for fk in desired.foreign_keys
        )
        actions: tuple[Action, ...] = (CreateTable(desired), *foreign_key_actions)
    else:
        actions = (
            _diff_columns(desired.columns, observed.columns)
            + _diff_properties(desired.properties, observed.properties)
            + _diff_table_comment(desired.comment, observed.comment)
            + _diff_partitioning(desired.partitioned_by, observed.partitioned_by)
            + _diff_primary_key(desired, observed)
            + _diff_foreign_keys(desired, observed)
        )
    return ActionPlan(actions)


def _diff_columns(desired: tuple[Column, ...], observed: tuple[Column, ...]) -> tuple[Action, ...]:
    """Return the column-level actions to transform `observed` into `desired`."""
    observed_by_name = {column.name: column for column in observed}
    desired_names = {column.name for column in desired}

    added = tuple(column for column in desired if column.name not in observed_by_name)
    dropped = tuple(column for column in observed if column.name not in desired_names)
    common = tuple(
        (desired_column, observed_by_name[desired_column.name])
        for desired_column in desired
        if desired_column.name in observed_by_name
    )

    add_actions = tuple(AddColumn(column=column) for column in added)
    drop_actions = tuple(DropColumn(column.name) for column in dropped)
    comment_actions = tuple(
        SetColumnComment(desired_column.name, desired_column.comment)
        for desired_column, observed_column in common
        if desired_column.comment != observed_column.comment
    )
    nullability_actions = tuple(
        SetColumnNullability(column_name=desired_column.name, nullable=desired_column.nullable)
        for desired_column, observed_column in common
        if desired_column.nullable != observed_column.nullable
    )
    type_change_actions = tuple(
        ColumnTypeChange(
            column_name=desired_column.name,
            from_type=observed_column.data_type,
            to_type=desired_column.data_type,
        )
        for desired_column, observed_column in common
        if desired_column.data_type != observed_column.data_type
    )
    return add_actions + drop_actions + comment_actions + nullability_actions + type_change_actions


def _diff_properties(
    desired: Mapping[str, str], observed: Mapping[str, str]
) -> tuple[SetProperty, ...]:
    """
    Return the `SetProperty` actions needed to align observed with desired.

    Properties are a declared subset, not a complete desired state: the engine
    only manages keys the user declared. Observed-only keys (e.g. properties
    Databricks sets autonomously) are left untouched — they are never unset.
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


def _diff_primary_key(desired: DesiredTable, observed: ObservedTable) -> tuple[Action, ...]:
    """
    Return the primary key actions to align observed with desired.

    Uses frozenset comparison so column order does not trigger spurious changes.
    Declaration order from desired is preserved in SetPrimaryKey.columns.
    """
    desired_primary_key = frozenset(desired.primary_key)
    observed_primary_key = frozenset(observed.primary_key)

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


def _fk_content_equal(a: ForeignKeyConstraint, b: ForeignKeyConstraint) -> bool:
    """
    Compare two FK constraints by content only, ignoring constraint_name.

    This is necessary because a desired FK may have constraint_name=None while
    the catalog-observed FK carries the derived name. Using dataclass __eq__
    would treat them as different and cause spurious Drop+Set on every sync.
    """
    return (
        a.local_columns == b.local_columns
        and a.references == b.references
        and a.referenced_columns == b.referenced_columns
    )


def _diff_foreign_keys(desired: DesiredTable, observed: ObservedTable) -> tuple[Action, ...]:
    """
    Return the FK actions to align observed with desired.

    Foreign keys are matched by *content* (local columns, referenced table, and
    referenced columns), not by constraint name. An observed FK whose content is
    not desired is dropped; a desired FK whose content is not observed is set.
    A FK present on both sides — even under a different constraint name, e.g. one
    created outside this engine — produces no actions, so a sync over an
    unchanged catalog stays idempotent.

    Dropping an observed FK uses its catalog-stored constraint name so the
    correct constraint is removed; setting a desired FK uses its resolved name.
    """
    observed_unmatched = list(observed.foreign_keys)

    actions: list[Action] = []
    for desired_fk in desired.foreign_keys:
        match = next(
            (fk for fk in observed_unmatched if _fk_content_equal(desired_fk, fk)),
            None,
        )
        if match is None:
            actions.append(
                SetForeignKey(
                    foreign_key=desired_fk,
                    constraint_name=desired.resolve_foreign_key_constraint_name(desired_fk),
                )
            )
        else:
            observed_unmatched.remove(match)

    drop_actions = [
        DropForeignKey(constraint_name=fk.resolve_constraint_name(observed.qualified_name.name))
        for fk in observed_unmatched
    ]

    return tuple(drop_actions) + tuple(actions)
