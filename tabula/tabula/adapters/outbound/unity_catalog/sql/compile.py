from __future__ import annotations
from functools import singledispatch
from typing import Tuple
from tabula.domain.model.actions import ActionPlan, CreateTable, AddColumn, DropColumn
from tabula.domain.model.column import Column

def sql_type_for_column(column: Column) -> str:
    t = column.data_type.name.lower()
    if t == "string": return "STRING"
    if t == "integer": return "INT"
    if t == "small_integer": return "SMALLINT"
    if t == "big_integer": return "BIGINT"
    if t == "boolean": return "BOOLEAN"
    if t == "float": return "FLOAT"
    if t == "double": return "DOUBLE"
    if t == "date": return "DATE"
    if t == "timestamp": return "TIMESTAMP"
    if t == "decimal":
        precision, scale = column.data_type.parameters
        return f"DECIMAL({precision},{scale})"
    if t == "varchar":
        (length,) = column.data_type.parameters
        return f"STRING /* varchar({length}) */"
    return t.upper()

def column_definition(column: Column) -> str:
    return f"{column.name} {sql_type_for_column(column)}"

@singledispatch
def compile_action(action, *, full_name: str) -> str:
    raise NotImplementedError(f"No SQL compiler for {type(action).__name__}")

@compile_action.register
def _(action: CreateTable, *, full_name: str) -> str:
    cols = ", ".join(column_definition(c) for c in action.columns)
    return f"CREATE TABLE {full_name} ({cols})"

@compile_action.register
def _(action: AddColumn, *, full_name: str) -> str:
    return f"ALTER TABLE {full_name} ADD COLUMNS ({column_definition(action.column)})"

@compile_action.register
def _(action: DropColumn, *, full_name: str) -> str:
    return f"ALTER TABLE {full_name} DROP COLUMN {action.column_name}"

def compile_plan(plan: ActionPlan) -> Tuple[str, ...]:
    full_name = plan.full_name.qualified_name()
    return tuple(compile_action(a, full_name=full_name) for a in plan.actions)
