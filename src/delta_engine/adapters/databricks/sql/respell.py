"""
Normalise a plan's column spelling to the catalog's before rendering.

Every column reference the compiler renders is respelled: the catalog's
spelling where the column is known, the given spelling unchanged where it is
not (a column added by this plan, a rename target, a table created this
sync). The rule is uniform — no per-operation carve-outs — so emitted DDL is
correct whether or not a given Databricks path resolves identifiers
case-sensitively (``ADD CONSTRAINT`` does; ordinary DDL does not). Only the
rendered statements are normalised: the stored ``ActionPlan`` keeps its
declared, semantic spellings.
"""

from dataclasses import replace
from functools import singledispatch

from delta_engine.application.ports import CatalogSpellings
from delta_engine.domain.model import DesiredTable, PrimaryKeyConstraint, QualifiedName
from delta_engine.domain.plan import (
    Action,
    ActionPlan,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    RenameColumn,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    UnsetColumnTag,
)


def respell_plan(plan: ActionPlan, spellings: CatalogSpellings) -> ActionPlan:
    """Return ``plan`` with every rendered column reference catalog-spelled."""
    return replace(
        plan,
        actions=tuple(_respell(action, plan.target, spellings) for action in plan),
    )


@singledispatch
def _respell(action: Action, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    """Actions that render no column references pass through unchanged."""
    return action


@_respell.register
def _(action: AddColumn, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    column = action.column
    return replace(action, column=replace(column, name=spellings.spelling(target, column.name)))


@_respell.register
def _(action: DropColumn, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    column = action.column
    return replace(action, column=replace(column, name=spellings.spelling(target, column.name)))


@_respell.register
def _(action: RenameColumn, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(
        action,
        old_name=spellings.spelling(target, action.old_name),
        new_name=spellings.spelling(target, action.new_name),
    )


@_respell.register
def _(action: SetColumnComment, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(action, column_name=spellings.spelling(target, action.column_name))


@_respell.register
def _(action: SetColumnNullability, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(action, column_name=spellings.spelling(target, action.column_name))


@_respell.register
def _(action: SetColumnTag, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(action, column_name=spellings.spelling(target, action.column_name))


@_respell.register
def _(action: UnsetColumnTag, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(action, column_name=spellings.spelling(target, action.column_name))


@_respell.register
def _(action: AlterColumnType, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(action, column_name=spellings.spelling(target, action.column_name))


@_respell.register
def _(action: AlterClustering, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(
        action,
        desired_clustering=tuple(
            spellings.spelling(target, column) for column in action.desired_clustering
        ),
    )


@_respell.register
def _(action: SetPrimaryKey, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(action, primary_key=_respell_primary_key(action.primary_key, target, spellings))


@_respell.register
def _(action: SetForeignKey, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    constraint = action.constraint
    return replace(
        action,
        constraint=replace(
            constraint,
            local_columns=tuple(
                spellings.spelling(target, column) for column in constraint.local_columns
            ),
            referenced_columns=tuple(
                spellings.spelling(constraint.referenced_table, column)
                for column in constraint.referenced_columns
            ),
        ),
    )


@_respell.register
def _(action: CreateTable, target: QualifiedName, spellings: CatalogSpellings) -> Action:
    return replace(action, table=_respell_created_table(action.table, spellings))


def _respell_primary_key(
    primary_key: PrimaryKeyConstraint, table: QualifiedName, spellings: CatalogSpellings
) -> PrimaryKeyConstraint:
    return replace(
        primary_key,
        columns=tuple(spellings.spelling(table, column) for column in primary_key.columns),
    )


def _respell_created_table(table: DesiredTable, spellings: CatalogSpellings) -> DesiredTable:
    """CREATE TABLE renders columns, layout, and the PK; foreign keys are not rendered."""
    name = table.qualified_name
    return replace(
        table,
        columns=tuple(
            replace(column, name=spellings.spelling(name, column.name)) for column in table.columns
        ),
        partitioned_by=tuple(spellings.spelling(name, column) for column in table.partitioned_by),
        clustered_by=tuple(spellings.spelling(name, column) for column in table.clustered_by),
        primary_key=(
            _respell_primary_key(table.primary_key, name, spellings)
            if table.primary_key is not None
            else None
        ),
    )
