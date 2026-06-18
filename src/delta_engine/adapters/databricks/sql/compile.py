"""
Compile domain action plans into Spark/Databricks SQL statements.

Uses `functools.singledispatch` to render SQL per action type and returns a
tuple of statements ready to execute against a Spark session.
"""

from __future__ import annotations

from collections.abc import Mapping
from functools import singledispatch

from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.adapters.databricks.sql.types import sql_type_for_data_type
from delta_engine.domain.model import QualifiedName
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
)


def compile_plan(qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
    """Compile an :class:`ActionPlan` for ``qualified_name`` into Spark SQL statements."""
    backticked_table_name = backtick_qualified_name(qualified_name)
    return tuple(_compile_action(action, backticked_table_name) for action in plan)


@singledispatch
def _compile_action(action: Action, backticked_table_name: str) -> str:
    """Dispatch to action-specific SQL compiler."""
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")


@_compile_action.register
def _(action: CreateTable, backticked_table_name: str) -> str:
    """Compile a CREATE TABLE statement including columns, comment, and properties."""
    table = action.table
    columns = ", ".join(_column_definition(c) for c in table.columns)
    table_comment = _set_table_comment(table.comment)
    properties = _set_properties(table.properties)
    partition_by = _set_partitioned_by(table.partitioned_by)

    parts = [
        f"CREATE TABLE IF NOT EXISTS {backticked_table_name}",
        f"({columns})",
        "USING delta",
        table_comment,
        properties,
        partition_by,
    ]
    return " ".join(p for p in parts if p)


@_compile_action.register
def _(action: AddColumn, backticked_table_name: str) -> str:
    """
    Compile an ALTER TABLE ... ADD COLUMN statement for a single column.

    The column is always added without a NOT NULL constraint: adding a
    non-nullable column to an existing table is rejected at validation
    (see NonNullableColumnAdd), so this path is only reached for nullable adds.
    """
    name = backtick(action.column.name)
    dtype = sql_type_for_data_type(action.column.data_type)
    comment = f" COMMENT {quote_literal(action.column.comment)}" if action.column.comment else ""
    return f"ALTER TABLE {backticked_table_name} ADD COLUMN {name} {dtype}{comment}"


@_compile_action.register
def _(action: DropColumn, backticked_table_name: str) -> str:
    """Compile an ALTER TABLE ... DROP COLUMN statement for a column name."""
    column_name = backtick(action.column_name)
    return f"ALTER TABLE {backticked_table_name} DROP COLUMN {column_name}"


@_compile_action.register
def _(action: SetProperty, backticked_table_name: str) -> str:
    pair = f"{quote_literal(action.name)}={quote_literal(action.value)}"
    return f"ALTER TABLE {backticked_table_name} SET TBLPROPERTIES ({pair})"


@_compile_action.register
def _(action: SetColumnComment, backticked_table_name: str) -> str:
    column_name = backtick(action.column_name)
    comment = quote_literal(action.comment)
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column_name} COMMENT {comment}"


@_compile_action.register
def _(action: SetTableComment, backticked_table_name: str) -> str:
    comment = quote_literal(action.comment)
    return f"COMMENT ON TABLE {backticked_table_name} IS {comment}"


@_compile_action.register
def _(action: SetColumnNullability, backticked_table_name: str) -> str:
    column_name = backtick(action.column_name)
    sign = "DROP" if action.nullable else "SET"
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column_name} {sign} NOT NULL"


# ----------- helpers ------------


def _column_definition(column) -> str:
    """Render a single column definition fragment, including its comment."""
    column_name = backtick(column.name)
    type = sql_type_for_data_type(column.data_type)
    nullable = "" if column.nullable else "NOT NULL"
    comment = f"COMMENT {quote_literal(column.comment)}" if column.comment else ""
    return " ".join(part for part in (column_name, type, nullable, comment) if part)


def _set_table_comment(comment: str) -> str:
    """Render the table COMMENT clause, or '' when there is no comment to set."""
    if not comment:
        return ""
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
