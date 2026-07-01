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
from delta_engine.domain.model import Column, QualifiedName
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    UnsupportedChange,
)


def compile_plan(qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
    """Compile an :class:`ActionPlan` for ``qualified_name`` into Spark SQL statements."""
    backticked_table_name = backtick_qualified_name(qualified_name)
    table_name = qualified_name.name
    return tuple(_compile_action(action, backticked_table_name, table_name) for action in plan)


@singledispatch
def _compile_action(action: Action, backticked_table_name: str, table_name: str) -> str:
    """Dispatch to action-specific SQL compiler."""
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")


@_compile_action.register
def _(action: CreateTable, backticked_table_name: str, table_name: str) -> str:
    """Compile a CREATE TABLE statement including columns, comment, properties, and optional PK."""
    table = action.table
    column_defs = [_column_definition(column) for column in table.columns]

    if table.primary_key is not None:
        pk_cols = ", ".join(backtick(name) for name in table.primary_key.columns)
        constraint_name = derive_constraint_name(table.qualified_name.name, None)
        column_defs.append(f"CONSTRAINT {backtick(constraint_name)} PRIMARY KEY ({pk_cols})")

    columns_clause = ", ".join(column_defs)
    table_comment = _set_table_comment(table.comment)
    properties = _set_properties(table.properties)
    partition_by = _set_partitioned_by(table.partitioned_by)

    # IF NOT EXISTS, even though CreateTable is only emitted after the reader
    # reports the table absent. It guards the read-then-create race: if another
    # process creates the table in that window, the statement no-ops rather than
    # erroring. The trade-off is that such a table is reported created without
    # reconciling its schema; the next sync re-reads and plans any drift. This
    # favours a resilient run over failing loud on a rare race -- see the README
    # non-goals.
    parts = [
        f"CREATE TABLE IF NOT EXISTS {backticked_table_name}",
        f"({columns_clause})",
        "USING delta",
        table_comment,
        properties,
        partition_by,
    ]
    return " ".join(p for p in parts if p)


@_compile_action.register
def _(action: AddColumn, backticked_table_name: str, table_name: str) -> str:
    """
    Compile an ALTER TABLE ... ADD COLUMN statement for a single column.

    The column is always added without a NOT NULL constraint: adding a
    non-nullable column to an existing table is rejected at validation
    (see NonNullableColumnAdd), so this path is only reached for nullable adds.
    The guard makes that contract loud -- it fires only if validation was
    bypassed or a custom rule set let a NOT NULL add through, rather than
    silently emitting an add that drops the constraint. It is an unconditional
    ``raise`` (not ``assert``) so the invariant survives ``python -O``, matching
    the UnsupportedChange guard below.
    """
    if not action.column.nullable:
        raise AssertionError(
            f"AddColumn reached the compiler with non-nullable column {action.column.name!r}; "
            "validation (NonNullableColumnAdd) should have blocked this"
        )
    name = backtick(action.column.name)
    dtype = sql_type_for_data_type(action.column.data_type)
    comment = f" COMMENT {quote_literal(action.column.comment)}" if action.column.comment else ""
    return f"ALTER TABLE {backticked_table_name} ADD COLUMN {name} {dtype}{comment}"


@_compile_action.register
def _(action: DropColumn, backticked_table_name: str, table_name: str) -> str:
    """Compile an ALTER TABLE ... DROP COLUMN statement for a column name."""
    column_name = backtick(action.column_name)
    return f"ALTER TABLE {backticked_table_name} DROP COLUMN {column_name}"


@_compile_action.register
def _(action: SetProperty, backticked_table_name: str, table_name: str) -> str:
    pair = f"{quote_literal(action.name)}={quote_literal(action.value)}"
    return f"ALTER TABLE {backticked_table_name} SET TBLPROPERTIES ({pair})"


@_compile_action.register
def _(action: SetColumnComment, backticked_table_name: str, table_name: str) -> str:
    column_name = backtick(action.column_name)
    if not action.comment:
        return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column_name} UNSET COMMENT"
    comment = quote_literal(action.comment)
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column_name} COMMENT {comment}"


@_compile_action.register
def _(action: SetTableComment, backticked_table_name: str, table_name: str) -> str:
    comment = quote_literal(action.comment)
    return f"COMMENT ON TABLE {backticked_table_name} IS {comment}"


@_compile_action.register
def _(action: SetColumnNullability, backticked_table_name: str, table_name: str) -> str:
    column_name = backtick(action.column_name)
    sign = "DROP" if action.nullable else "SET"
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column_name} {sign} NOT NULL"


@_compile_action.register
def _(action: DropPrimaryKey, backticked_table_name: str, table_name: str) -> str:
    """Compile an ALTER TABLE ... DROP PRIMARY KEY IF EXISTS statement."""
    return f"ALTER TABLE {backticked_table_name} DROP PRIMARY KEY IF EXISTS"


@_compile_action.register
def _(action: SetPrimaryKey, backticked_table_name: str, table_name: str) -> str:
    """Compile an ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY statement."""
    column_clause = ", ".join(backtick(column.name) for column in action.columns)
    constraint = backtick(derive_constraint_name(table_name, None))
    return (
        f"ALTER TABLE {backticked_table_name}"
        f" ADD CONSTRAINT {constraint} PRIMARY KEY ({column_clause})"
    )


@_compile_action.register
def _(action: DropForeignKey, backticked_table_name: str, table_name: str) -> str:
    """Compile ALTER TABLE ... DROP CONSTRAINT IF EXISTS for a foreign key."""
    constraint = backtick(action.constraint_name)
    return f"ALTER TABLE {backticked_table_name} DROP CONSTRAINT IF EXISTS {constraint}"


@_compile_action.register
def _(action: SetForeignKey, backticked_table_name: str, table_name: str) -> str:
    """Compile ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ..."""
    foreign_key = action.foreign_key
    constraint = backtick(derive_constraint_name(table_name, foreign_key.local_columns))
    local_cols = ", ".join(backtick(col) for col in foreign_key.local_columns)
    ref_cols = ", ".join(backtick(col) for col in foreign_key.referenced_columns)
    # references is a dotted qualified name — split and backtick each part
    backticked_ref = ".".join(backtick(part) for part in foreign_key.references.split("."))
    return (
        f"ALTER TABLE {backticked_table_name}"
        f" ADD CONSTRAINT {constraint}"
        f" FOREIGN KEY ({local_cols}) REFERENCES {backticked_ref} ({ref_cols})"
    )


@_compile_action.register
def _(action: UnsupportedChange, backticked_table_name: str, table_name: str) -> str:
    # Validation rejects this action before execution, so reaching here is an
    # internal-invariant violation (AssertionError), not an unimplemented feature.
    raise AssertionError(
        f"Unsupported change reached the compiler: {action.kind.value}"
        f" on '{action.subject_name}' ({action.from_repr} -> {action.to_repr})."
        " Recreate the table to apply this change."
    )


# ----------- helpers ------------


def derive_constraint_name(table_name: str, local_columns: tuple[str, ...] | None) -> str:
    """
    Derive the SQL constraint name for a table-owned constraint.

    This is the engine's naming policy, kept in the adapter: the domain does
    not know how a constraint is named. ``local_columns is None`` means the
    primary key (``{table}_pk``); otherwise a foreign key over those columns
    (``{table}_{col1_col2_...}_fk``).
    """
    if local_columns is None:
        return f"{table_name}_pk"
    columns = "_".join(local_columns)
    return f"{table_name}_{columns}_fk"


def _column_definition(column: Column) -> str:
    """Render a single column definition fragment, including its comment."""
    column_name = backtick(column.name)
    sql_type = sql_type_for_data_type(column.data_type)
    nullable = "" if column.nullable else "NOT NULL"
    comment = f"COMMENT {quote_literal(column.comment)}" if column.comment else ""
    return " ".join(part for part in (column_name, sql_type, nullable, comment) if part)


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
    quoted_columns = ", ".join(backtick(column_name) for column_name in partitioned_by)
    return f"PARTITIONED BY ({quoted_columns})"
