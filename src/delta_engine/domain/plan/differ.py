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
        fk_actions = tuple(
            SetForeignKey(fk=fk, constraint_name=desired.foreign_key_constraint_name(fk))
            for fk in desired.foreign_keys
        )
        actions: tuple[Action, ...] = (CreateTable(desired),) + fk_actions
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
    desired_set = frozenset(desired.primary_key)
    observed_set = frozenset(observed.primary_key)

    if desired_set == observed_set:
        return ()

    actions: list[Action] = []

    if observed_set:
        actions.append(DropPrimaryKey())

    if desired_set:
        pk_columns = tuple(column for column in desired.columns if column.name in desired_set)
        constraint_name = desired.primary_key_constraint_name
        assert constraint_name is not None  # guaranteed: desired_set is non-empty
        actions.append(
            SetPrimaryKey(
                columns=pk_columns,
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

    Each FK is keyed by its resolved constraint name. When a FK exists under the
    same name in both desired and observed but its definition has changed, a
    DropForeignKey is emitted for the old name followed by a SetForeignKey for the
    new definition. A FK that is identical on both sides produces no actions.
    """
    desired_by_name = {
        desired.foreign_key_constraint_name(fk): fk for fk in desired.foreign_keys
    }
    observed_by_name = {
        fk.resolved_constraint_name(observed.qualified_name.name): fk
        for fk in observed.foreign_keys
    }

    actions: list[Action] = []

    for name, fk in observed_by_name.items():
        if name not in desired_by_name or not _fk_content_equal(desired_by_name[name], fk):
            actions.append(DropForeignKey(constraint_name=name))

    for name, fk in desired_by_name.items():
        if name not in observed_by_name or not _fk_content_equal(observed_by_name[name], fk):
            actions.append(SetForeignKey(fk=fk, constraint_name=name))

    return tuple(actions)
