from __future__ import annotations

from tabula.domain.plan.actions import ActionPlan, CreateTable, AddColumn, DropColumn
from tabula.adapters.databricks.sql.types import sql_type_for_data_type
from tabula.adapters.databricks.sql.dialects import SqlDialect, SPARK_SQL


def compile_plan(plan: ActionPlan, *, dialect: SqlDialect = SPARK_SQL) -> tuple[str, ...]:
    """
    Compile an ActionPlan into ordered SQL statements for the given dialect.
    1 action -> 1 statement (by design).
    """
    full_table_name = dialect.render_qualified_name(
        plan.qualified_name.catalog,
        plan.qualified_name.schema,
        plan.qualified_name.name,
    )
    return tuple(_compile_action(a, full_table_name=full_table_name, dialect=dialect) for a in plan)


def _compile_action(action: object, *, full_table_name: str, dialect: SqlDialect) -> str:
    """
    Translate a single action to a single SQL statement.
    """
    match action:
        case CreateTable(columns=columns):
            cols_sql = ", ".join(_column_definition(c, dialect=dialect) for c in columns)
            return f"CREATE TABLE IF NOT EXISTS {full_table_name} ({cols_sql})"

        case AddColumn(column=column):
            col_sql = _column_definition(column, dialect=dialect)
            return f"ALTER TABLE {full_table_name} ADD COLUMN IF NOT EXISTS {col_sql}"

        case DropColumn(column_name=col_name):
            col_ident = dialect.quote_identifier(col_name)
            return f"ALTER TABLE {full_table_name} DROP COLUMN IF EXISTS {col_ident}"

        case _:
            raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")


def _column_definition(column, *, dialect: SqlDialect) -> str:
    """
    Render `<quoted_name> <engine_type> [NULL|NOT NULL]`.
    """
    name_sql = dialect.quote_identifier(column.name)
    type_sql = sql_type_for_data_type(column)
    nullability = "NULL" if getattr(column, "is_nullable", True) else "NOT NULL"
    return f"{name_sql} {type_sql} {nullability}"
