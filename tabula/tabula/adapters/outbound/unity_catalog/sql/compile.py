"""Compile domain Actions into Databricks UC/Delta SQL.

Policies:
- Identifiers are always backtick-quoted and backticks are escaped (`` -> ```).
- Idempotency: use IF [NOT] EXISTS where applicable.
- Nullability: force NOT NULL on CREATE and ADD (a later step will relax nullability).
- Single-column ADD/DROP for simplicity (TODO batch later).
"""

from __future__ import annotations
from functools import singledispatch
from typing import Tuple

from tabula.domain.model.actions import ActionPlan, Action, CreateTable, AddColumn, DropColumn
from tabula.domain.model.column import Column
from tabula.domain.model.qualified_name import QualifiedName

# ---- Identifier quoting -------------------------------------------------------

def _quote_ident(part: str) -> str:
    """Backtick-quote an identifier part and escape embedded backticks."""
    return f"`{part.replace('`', '``')}`"

def _table_ref(qn: QualifiedName) -> str:
    """Return `catalog`.`schema`.`name` with backticks."""
    return ".".join(_quote_ident(p) for p in (qn.catalog, qn.schema, qn.name))

# ---- Type mapping ----------------------------------------

_TYPE_SQL = {
    "string": "STRING",
    "boolean": "BOOLEAN",
    "int": "INT",
    "smallint": "SMALLINT",
    "bigint": "BIGINT",
    "float": "FLOAT",     # 32-bit
    "double": "DOUBLE",   # 64-bit
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "varchar": "STRING",  # we annotate length in a comment if provided
}

def _sql_type_for_column(column: Column) -> str:
    name = column.data_type.name  # should already be lowercased in the domain
    if name == "varchar" and column.data_type.parameters:
        (length,) = column.data_type.parameters
        return f"STRING /* varchar({length}) */"
    try:
        return _TYPE_SQL[name]
    except KeyError:
        # Fail fast instead of silently uppercasing unknown types
        raise ValueError(
            f"Unsupported data type for UC compiler: {column.data_type.specification}"
        )

def _column_def(column: Column) -> str:
    """Always NOT NULL by policy (can be relaxed later by a dedicated step)."""
    return f"{_quote_ident(column.name)} {_sql_type_for_column(column)} NOT NULL"

# ---- Public API ---------------------------------------------------

def compile_plan(plan: ActionPlan) -> Tuple[str, ...]:
    """Compile an ActionPlan into one or more idempotent SQL statements."""
    table = _table_ref(plan.qualified_name)
    statements: list[str] = []
    for action in plan:
        statements.extend(_compile_action(action, table=table))
    return tuple(statements)

@singledispatch
def _compile_action(action: Action, *, table: str) -> Tuple[str, ...]:
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")

@_compile_action.register
def _(action: CreateTable, *, table: str) -> Tuple[str, ...]:
    cols = ", ".join(_column_def(c) for c in action.columns)
    return (f"CREATE TABLE IF NOT EXISTS {table} ({cols})",)

@_compile_action.register
def _(action: AddColumn, *, table: str) -> Tuple[str, ...]:
    # NOTE: single-column ADD for MVP.  TODO: batch multiple adds.
    col = _column_def(action.column)
    return (f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col}",)

@_compile_action.register
def _(action: DropColumn, *, table: str) -> Tuple[str, ...]:
    col = _quote_ident(action.column_name)
    return (f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col}",)