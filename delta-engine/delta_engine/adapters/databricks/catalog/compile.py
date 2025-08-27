from __future__ import annotations

from functools import singledispatch

from delta_engine.adapters.databricks.sql import (
    quote_identifier,
    render_qualified_name,
    sql_type_for_data_type,
)
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
)

# ----------------------------
# Public entry point
# ----------------------------

def compile_plan(plan: ActionPlan) -> tuple[str, ...]:
    """Compile an *ordered* ActionPlan into SQL statements.

    Args:
        plan: Ordered actions to convert into SQL.

    Returns:
        Tuple of SQL statements corresponding to the actions.

    """
    qn = plan.target # QualifiedName
    table_name_sql = render_qualified_name(qn.catalog, qn.schema, qn.name)
    return tuple(_compile_action(action, table_name_sql) for action in plan)


# ----------------------------
# Internal helpers
# ----------------------------


@singledispatch
def _compile_action(action: Action, full_table_name: str) -> str:  # pragma: no cover
    """Dispatch to action-specific SQL compiler."""
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")


@_compile_action.register
def _(action: CreateTable, full_table_name: str) -> str:
    columns_sql = ", ".join(_column_def(c) for c in action.columns)
    return f"CREATE TABLE IF NOT EXISTS {full_table_name} ({columns_sql})"


@_compile_action.register
def _(action: AddColumn, full_table_name: str) -> str:
    column_sql = _column_def(action.column)
    return f"ALTER TABLE {full_table_name} ADD COLUMN {column_sql}"


@_compile_action.register
def _(action: DropColumn, full_table_name: str) -> str:
    column_ident = quote_identifier(action.column_name)
    return f"ALTER TABLE {full_table_name} DROP COLUMN {column_ident}"


def _column_def(column) -> str:
    """Render a single column definition fragment."""
    name_sql = quote_identifier(column.name)
    type_sql = sql_type_for_data_type(column.data_type)
    nullable_sql = "" if column.is_nullable else "NOT NULL"
    return f"{name_sql} {type_sql} {nullable_sql}"
