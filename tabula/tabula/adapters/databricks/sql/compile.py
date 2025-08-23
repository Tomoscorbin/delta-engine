from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import singledispatch

from tabula.adapters.databricks.sql.dialects import SPARK_SQL, SqlDialect
from tabula.adapters.databricks.sql.types import sql_type_for_data_type
from tabula.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
)


@dataclass(frozen=True, slots=True)
class CompileContext:
    """Execution context for compiling a plan to SQL."""

    full_table_name: str
    quote_identifier: Callable[[str], str]


def compile_plan(plan: ActionPlan, *, dialect: SqlDialect = SPARK_SQL) -> tuple[str, ...]:
    """
    Compile an ActionPlan into ordered SQL statements for the given dialect.
    1 action -> 1 statement.
    """
    context = _make_context(plan, dialect)
    return tuple(compile_action(action, context) for action in plan)


def _make_context(plan: ActionPlan, dialect: SqlDialect) -> CompileContext:
    """Precompute the pieces we need."""
    qualified_name = plan.target
    sql_table_name = dialect.render_qualified_name(
        qualified_name.catalog,
        qualified_name.schema,
        qualified_name.name,
    )
    return CompileContext(
        full_table_name=sql_table_name,
        quote_identifier=dialect.quote_identifier,
    )


@singledispatch
def compile_action(action: Action, context: CompileContext) -> str:
    """Dispatch by action type; adapter-only logic lives here."""
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")


@compile_action.register
def _(action: CreateTable, context: CompileContext) -> str:
    columns_sql = ", ".join(_column_definition(c, context) for c in action.columns)
    return f"CREATE TABLE IF NOT EXISTS {context.full_table_name} ({columns_sql})"


@compile_action.register
def _(action: AddColumn, context: CompileContext) -> str:
    column_sql = _column_definition(action.column, context)
    return f"ALTER TABLE {context.full_table_name} ADD COLUMN IF NOT EXISTS {column_sql}"


@compile_action.register
def _(action: DropColumn, context: CompileContext) -> str:
    column_ident = context.quote_identifier(action.column_name)
    return f"ALTER TABLE {context.full_table_name} DROP COLUMN IF EXISTS {column_ident}"


def _column_definition(column, context: CompileContext) -> str:
    """
    Render `<quoted_name> <engine_type> [NULL|NOT NULL]`.
    Dialect influence is hidden behind the context.
    """
    name_sql = context.quote_identifier(column.name)
    type_sql = sql_type_for_data_type(column.data_type)
    nullability = "NULL" if getattr(column, "is_nullable", True) else "NOT NULL"
    return f"{name_sql} {type_sql} {nullability}"
