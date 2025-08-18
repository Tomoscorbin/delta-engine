from __future__ import annotations
from functools import singledispatch

from tabula.domain.plan.actions import ActionPlan, CreateTable, AddColumn, DropColumn
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.column import Column

from tabula.adapters.databricks.sql.types import sql_type_for_data_type

from tabula.adapters.databricks.sql.dialects import (
    SqlDialect,
    SPARK_SQL,
    render_fully_qualified_name,
)

# ---- Public API -------------------------------------------------------------

def compile_plan(plan: ActionPlan, *, dialect: SqlDialect = SPARK_SQL) -> tuple[str, ...]:
    """Compile an ActionPlan into idempotent Spark SQL statements."""
    table_sql = render_fully_qualified_name(
        plan.qualified_name.catalog,
        plan.qualified_name.schema,
        plan.qualified_name.name,
        dialect=dialect,
    )
    statements: list[str] = []
    for action in plan:
        statements.extend(_compile_action(action, table=table_sql, dialect=dialect))
    return tuple(statements)

# ---- Single-dispatch core ---------------------------------------------------

@singledispatch
def _compile_action(action: object, *, table: str, dialect: SqlDialect) -> tuple[str, ...]:
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")

# ---- Concrete actions -------------------------------------------------------

@_compile_action.register
def _(action: CreateTable, *, table: str, dialect: SqlDialect) -> tuple[str, ...]:
    columns_sql = ", ".join(_column_definition(c, dialect=dialect) for c in action.columns)
    return (f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql})",) #TODO: choose format

@_compile_action.register
def _(action: AddColumn, *, table: str, dialect: SqlDialect) -> tuple[str, ...]:
    # TODO: later batch with ADD COLUMNS (col1..., col2...)
    col_sql = _column_definition(action.column, dialect=dialect)
    return (f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_sql}",)

@_compile_action.register
def _(action: DropColumn, *, table: str, dialect: SqlDialect) -> tuple[str, ...]:
    col_ident = dialect.quote_identifier(action.column_name)
    return (f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col_ident}",)

# ---- Helpers ----------------------------------------------------------------

def _column_definition(column, *, dialect: SqlDialect) -> str:
    """
    Render a single column definition as <quoted_name> <engine_type> NOT NULL.
    """
    column_identifier = dialect.quote_identifier(column.name)
    engine_sql_type = sql_type_for_data_type(column)
    return f"{column_identifier} {engine_sql_type} NOT NULL"