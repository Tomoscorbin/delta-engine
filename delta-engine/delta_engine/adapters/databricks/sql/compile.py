"""
Compile domain action plans into Spark/Databricks SQL statements.

Uses `functools.singledispatch` to render SQL per action type and returns a
tuple of statements ready to execute against a Spark session.
"""

from __future__ import annotations

from functools import singledispatch

from delta_engine.adapters.databricks.sql import (
    quote_identifier,
    quote_literal,
    quote_qualified_name,
    sql_type_for_data_type,
)
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetProperty,
    SetTableComment,
    UnsetProperty,
)


def compile_plan(plan: ActionPlan) -> tuple[str, ...]:
    """Compile an :class:`ActionPlan` into Spark SQL statements."""
    quoted_table_name = quote_qualified_name(plan.target)
    return tuple(_compile_action(action, quoted_table_name) for action in plan)


@singledispatch
def _compile_action(action: Action, _) -> str:
    """Dispatch to action-specific SQL compiler."""
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")


@_compile_action.register
def _(action: CreateTable, quoted_table_name: str) -> str:
    """Compile a CREATE TABLE statement with the plan's column definitions."""
    column_name = ", ".join(_column_def(c) for c in action.columns)
    return f"CREATE TABLE IF NOT EXISTS {quoted_table_name} ({column_name})"


@_compile_action.register
def _(action: AddColumn, quoted_table_name: str) -> str:
    """Compile an ALTER TABLE ... ADD COLUMN statement for a single column."""
    column_name = _column_def(action.column)
    return f"ALTER TABLE {quoted_table_name} ADD COLUMN {column_name}"
    # TODO: explicitly add new columns as nullable and then tighten in a nullability step


@_compile_action.register
def _(action: DropColumn, quoted_table_name: str) -> str:
    """Compile an ALTER TABLE ... DROP COLUMN statement for a column name."""
    column_name = quote_identifier(action.column_name)
    return f"ALTER TABLE {quoted_table_name} DROP COLUMN {column_name}"


@_compile_action.register
def _(action: SetProperty, quoted_table_name: str) -> str:
    pair = f"{quote_literal(action.name)}={quote_literal(action.value)}"
    return f"ALTER TABLE {quoted_table_name} SET TBLPROPERTIES ({pair})"


@_compile_action.register
def _(action: UnsetProperty, quoted_table_name: str) -> str:
    key = quote_literal(action.name)
    return f"ALTER TABLE {quoted_table_name} UNSET TBLPROPERTIES ({key})"


@_compile_action.register
def _(action: SetColumnComment, quoted_table_name: str) -> str:
    column_name = quote_identifier(action.column_name)
    comment = quote_literal(action.comment)
    return f"ALTER TABLE {quoted_table_name} ALTER COLUMN {column_name} COMMENT {comment}"


@_compile_action.register
def _(action: SetTableComment, quoted_table_name: str) -> str:
    comment = quote_literal(action.comment)
    return f"COMMENT ON TABLE {quoted_table_name} IS {comment}"


# ----------- helpers ------------


def _column_def(column) -> str:
    """Render a single column definition fragment."""
    name_sql = quote_identifier(column.name)
    type_sql = sql_type_for_data_type(column.data_type)
    nullable_sql = "" if column.is_nullable else "NOT NULL"
    return f"{name_sql} {type_sql} {nullable_sql}".strip()
