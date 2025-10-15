"""
Compile domain action plans into Spark/Databricks SQL statements.

Uses `functools.singledispatch` to render SQL per action type and returns a
tuple of statements ready to execute against a Spark session.
"""

from __future__ import annotations

from collections.abc import Mapping
from functools import singledispatch

from delta_engine.adapters.databricks.sql import (
    backtick,
    backtick_qualified_name,
    quote_literal,
    sql_type_for_data_type,
)
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetColumnNullability,
    SetProperty,
    SetTableComment,
    UnsetProperty,
)


def compile_plan(plan: ActionPlan) -> tuple[str, ...]:
    """Compile an :class:`ActionPlan` into Spark SQL statements."""
    quoted_table_name = backtick_qualified_name(plan.target)
    return tuple(_compile_action(action, quoted_table_name) for action in plan)


@singledispatch
def _compile_action(
    action: Action, quoted_table_name: str
) -> str:  # TODO: fix unaccessed quoted_table_name
    """Dispatch to action-specific SQL compiler."""
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")


@_compile_action.register
def _(action: CreateTable, quoted_table_name: str) -> str:
    """Compile a CREATE TABLE statement including columns, comment, and properties."""
    table = action.table
    columns = ", ".join(_column_definition(c) for c in table.columns)
    table_comment = _set_table_comment(table.comment)
    properties = _set_properties(table.properties)
    partition_by = _set_partitioned_by(table.partitioned_by)

    parts = [
        f"CREATE TABLE IF NOT EXISTS {quoted_table_name}",
        f"({columns})",
        f"USING {table.format}",
        table_comment,
        properties,
        partition_by,
    ]
    return " ".join(p for p in parts if p)


@_compile_action.register
def _(action: AddColumn, quoted_table_name: str) -> str:
    """
    Compile an ALTER TABLE ... ADD COLUMN statement for a single column.

    Note: New columns are added as nullable and then tightened later.
    """
    name = backtick(action.column.name)
    dtype = sql_type_for_data_type(action.column.data_type)
    comment = quote_literal(action.column.comment)
    return f"ALTER TABLE {quoted_table_name} ADD COLUMN {name} {dtype} COMMENT {comment}"


@_compile_action.register
def _(action: DropColumn, quoted_table_name: str) -> str:
    """Compile an ALTER TABLE ... DROP COLUMN statement for a column name."""
    column_name = backtick(action.column_name)
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
    column_name = backtick(action.column_name)
    comment = quote_literal(action.comment)
    return f"ALTER TABLE {quoted_table_name} ALTER COLUMN {column_name} COMMENT {comment}"


@_compile_action.register
def _(action: SetTableComment, quoted_table_name: str) -> str:
    comment = quote_literal(action.comment)
    return f"COMMENT ON TABLE {quoted_table_name} IS {comment}"


@_compile_action.register
def _(action: SetColumnNullability, quoted_table_name: str) -> str:
    column_name = backtick(action.column_name)
    sign = "DROP" if action.nullable else "SET"
    return f"ALTER TABLE {quoted_table_name} ALTER COLUMN {column_name} {sign} NOT NULL"


# ----------- helpers ------------


def _column_definition(column) -> str:
    """Render a single column definition fragment."""
    column_name = backtick(column.name)
    type = sql_type_for_data_type(column.data_type)
    nullable = "" if column.nullable else "NOT NULL"  # TODO: or be explicit?
    return f"{column_name} {type} {nullable}".strip()


def _set_table_comment(comment: str) -> str:
    return f"COMMENT {quote_literal(comment)}"


def _set_properties(props: Mapping[str, str] | None) -> str:
    if not props:
        return ""
    pairs = ", ".join(f"{quote_literal(k)}={quote_literal(v)}" for k, v in sorted(props.items()))
    return f"TBLPROPERTIES ({pairs})"


def _set_partitioned_by(partitioned_by: tuple[str, ...] = ()) -> str:
    """Return PARTITIONED BY (...) or '' if unpartitioned."""
    if not partitioned_by:
        return ""
    cols = ", ".join(backtick(c) for c in partitioned_by)
    return f"PARTITIONED BY ({cols})"
