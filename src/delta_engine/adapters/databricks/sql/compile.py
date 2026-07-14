"""
Compile domain action plans into Spark/Databricks SQL statements.

Uses `functools.singledispatch` to render SQL per action type and returns the
statements in plan order, ready to execute against a Spark session.
"""

from collections.abc import Mapping
from functools import singledispatch

from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.adapters.databricks.sql.types import render_data_type
from delta_engine.domain.model import DesiredColumn, QualifiedName
from delta_engine.domain.plan import (
    Action,
    ActionPlan,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    RenameColumn,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
)


def compile_plan(qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
    """Compile an :class:`ActionPlan` for ``qualified_name`` into SQL statements, in plan order."""
    backticked_table_name = backtick_qualified_name(qualified_name)
    return tuple(_compile_action(action, backticked_table_name) for action in plan)


@singledispatch
def _compile_action(action: Action, backticked_table_name: str) -> str:
    """Dispatch to action-specific SQL compiler."""
    raise NotImplementedError(f"No SQL compiler for action {type(action).__name__}")


@_compile_action.register
def _(action: CreateTable, backticked_table_name: str) -> str:
    """Compile a CREATE TABLE statement including columns, comment, properties, and optional PK."""
    table = action.table
    column_defs = [_column_definition(column) for column in table.columns]

    if table.primary_key is not None:
        pk_cols = ", ".join(backtick(name) for name in table.primary_key.columns)
        constraint_name = table.primary_key.constraint_name
        column_defs.append(f"CONSTRAINT {backtick(constraint_name)} PRIMARY KEY ({pk_cols})")

    columns_clause = ", ".join(column_defs)
    table_comment = _table_comment_clause(table.comment)
    properties = _properties_clause(table.properties)
    partition_by = _partitioned_by_clause(table.partitioned_by)
    cluster_by = _clustered_by_clause(table.clustered_by)

    # IF NOT EXISTS, even though CreateTable is only emitted after the reader
    # reports the table absent. It guards the read-then-create race: if another
    # process creates the table in that window, the statement no-ops rather than
    # erroring. The trade-off is that such a table is reported created without
    # reconciling its schema; the next sync re-reads and plans any drift. This
    # favours a resilient run over failing loud on a rare race. The resulting
    # false-success window is documented under concurrent catalog changes in
    # docs/reference-limitations.md.
    parts = [
        f"CREATE TABLE IF NOT EXISTS {backticked_table_name}",
        f"({columns_clause})",
        "USING delta",
        table_comment,
        properties,
        partition_by,
        cluster_by,
    ]
    return " ".join(p for p in parts if p)


@_compile_action.register
def _(action: AddColumn, backticked_table_name: str) -> str:
    """
    Compile an ALTER TABLE ... ADD COLUMN statement for a single column.

    The column is always added without a NOT NULL constraint: adding a
    non-nullable column to an existing table is rejected at validation
    (see NonNullableColumnAdd), so this path is only reached for nullable adds.
    The guard makes that contract loud -- it fires only if validation was
    bypassed or a custom rule set let a NOT NULL add through, rather than
    silently emitting an add that drops the constraint. It is an unconditional
    ``raise`` (not ``assert``) so the invariant survives ``python -O``.
    """
    if not action.column.nullable:
        raise AssertionError(
            f"AddColumn reached the compiler with non-nullable column {action.column.name!r}; "
            "validation (NonNullableColumnAdd) should have blocked this"
        )
    name = backtick(action.column.name)
    dtype = render_data_type(action.column.data_type)
    comment = f" COMMENT {quote_literal(action.column.comment)}" if action.column.comment else ""
    return f"ALTER TABLE {backticked_table_name} ADD COLUMN {name} {dtype}{comment}"


@_compile_action.register
def _(action: DropColumn, backticked_table_name: str) -> str:
    """Compile an ALTER TABLE ... DROP COLUMN statement for a column name."""
    column_name = backtick(action.column.name)
    return f"ALTER TABLE {backticked_table_name} DROP COLUMN {column_name}"


@_compile_action.register
def _(action: RenameColumn, backticked_table_name: str) -> str:
    """Compile ALTER TABLE ... RENAME COLUMN."""
    old = backtick(action.old_name)
    new = backtick(action.new_name)
    return f"ALTER TABLE {backticked_table_name} RENAME COLUMN {old} TO {new}"


@_compile_action.register
def _(action: SetProperty, backticked_table_name: str) -> str:
    pair = f"{quote_literal(action.name)}={quote_literal(action.desired_value)}"
    return f"ALTER TABLE {backticked_table_name} SET TBLPROPERTIES ({pair})"


@_compile_action.register
def _(action: UnsetProperty, backticked_table_name: str) -> str:
    key = quote_literal(action.name)
    return f"ALTER TABLE {backticked_table_name} UNSET TBLPROPERTIES IF EXISTS ({key})"


@_compile_action.register
def _(action: SetTableTag, backticked_table_name: str) -> str:
    pair = f"{quote_literal(action.name)}={quote_literal(action.value)}"
    return f"ALTER TABLE {backticked_table_name} SET TAGS ({pair})"


@_compile_action.register
def _(action: UnsetTableTag, backticked_table_name: str) -> str:
    return f"ALTER TABLE {backticked_table_name} UNSET TAGS ({quote_literal(action.name)})"


@_compile_action.register
def _(action: SetColumnTag, backticked_table_name: str) -> str:
    column = backtick(action.column_name)
    pair = f"{quote_literal(action.name)}={quote_literal(action.value)}"
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column} SET TAGS ({pair})"


@_compile_action.register
def _(action: UnsetColumnTag, backticked_table_name: str) -> str:
    column = backtick(action.column_name)
    return (
        f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column}"
        f" UNSET TAGS ({quote_literal(action.name)})"
    )


@_compile_action.register
def _(action: SetColumnComment, backticked_table_name: str) -> str:
    # An empty desired comment compiles to COMMENT '' rather than UNSET
    # COMMENT: SQL warehouses reject the latter, and '' round-trips as the
    # empty comment the reader observes, keeping resyncs idempotent.
    column_name = backtick(action.column_name)
    comment = quote_literal(action.desired_comment)
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column_name} COMMENT {comment}"


@_compile_action.register
def _(action: SetTableComment, backticked_table_name: str) -> str:
    comment = quote_literal(action.desired_comment)
    return f"COMMENT ON TABLE {backticked_table_name} IS {comment}"


@_compile_action.register
def _(action: SetColumnNullability, backticked_table_name: str) -> str:
    column_name = backtick(action.column_name)
    sign = "DROP" if action.desired_nullable else "SET"
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column_name} {sign} NOT NULL"


@_compile_action.register
def _(action: AlterClustering, backticked_table_name: str) -> str:
    """Compile ALTER TABLE ... CLUSTER BY (...) or CLUSTER BY NONE."""
    if not action.desired_clustering:
        return f"ALTER TABLE {backticked_table_name} CLUSTER BY NONE"
    columns = ", ".join(backtick(column) for column in action.desired_clustering)
    return f"ALTER TABLE {backticked_table_name} CLUSTER BY ({columns})"


@_compile_action.register
def _(action: AlterColumnType, backticked_table_name: str) -> str:
    """Compile ALTER TABLE ... ALTER COLUMN ... TYPE for a validated type widening."""
    column_name = backtick(action.column_name)
    sql_type = render_data_type(action.desired_type)
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {column_name} TYPE {sql_type}"


@_compile_action.register
def _(action: DropPrimaryKey, backticked_table_name: str) -> str:
    """Compile an ALTER TABLE ... DROP PRIMARY KEY IF EXISTS statement."""
    return f"ALTER TABLE {backticked_table_name} DROP PRIMARY KEY IF EXISTS"


@_compile_action.register
def _(action: SetPrimaryKey, backticked_table_name: str) -> str:
    """Compile an ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY statement."""
    column_clause = ", ".join(backtick(name) for name in action.primary_key.columns)
    constraint = backtick(action.primary_key.constraint_name)
    return (
        f"ALTER TABLE {backticked_table_name}"
        f" ADD CONSTRAINT {constraint} PRIMARY KEY ({column_clause})"
    )


@_compile_action.register
def _(action: DropForeignKey, backticked_table_name: str) -> str:
    """Compile ALTER TABLE ... DROP CONSTRAINT IF EXISTS for a foreign key."""
    constraint = backtick(action.constraint.constraint_name)
    return f"ALTER TABLE {backticked_table_name} DROP CONSTRAINT IF EXISTS {constraint}"


@_compile_action.register
def _(action: SetForeignKey, backticked_table_name: str) -> str:
    """Compile ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ..."""
    constraint = backtick(action.constraint.constraint_name)
    local_cols = ", ".join(backtick(col) for col in action.constraint.local_columns)
    ref_cols = ", ".join(backtick(col) for col in action.constraint.referenced_columns)
    backticked_ref = backtick_qualified_name(action.constraint.referenced_table)
    return (
        f"ALTER TABLE {backticked_table_name}"
        f" ADD CONSTRAINT {constraint}"
        f" FOREIGN KEY ({local_cols}) REFERENCES {backticked_ref} ({ref_cols})"
    )


# ----------- helpers ------------


def _column_definition(column: DesiredColumn) -> str:
    """Render a single column definition fragment, including its comment."""
    column_name = backtick(column.name)
    sql_type = render_data_type(column.data_type)
    nullable = "" if column.nullable else "NOT NULL"
    comment = f"COMMENT {quote_literal(column.comment)}" if column.comment else ""
    return " ".join(part for part in (column_name, sql_type, nullable, comment) if part)


def _table_comment_clause(comment: str) -> str:
    """Render the table COMMENT clause, or '' when there is no comment to set."""
    if not comment:
        return ""
    return f"COMMENT {quote_literal(comment)}"


def _properties_clause(props: Mapping[str, str | None]) -> str:
    # None values are absence assertions: a new table simply omits the key.
    pairs = ", ".join(
        f"{quote_literal(name)}={quote_literal(value)}"
        for name, value in sorted(props.items())
        if value is not None
    )
    if not pairs:
        return ""
    return f"TBLPROPERTIES ({pairs})"


def _partitioned_by_clause(partitioned_by: tuple[str, ...] = ()) -> str:
    """Return PARTITIONED BY (...) or '' if unpartitioned."""
    if not partitioned_by:
        return ""
    quoted_columns = ", ".join(backtick(column_name) for column_name in partitioned_by)
    return f"PARTITIONED BY ({quoted_columns})"


def _clustered_by_clause(clustered_by: tuple[str, ...] = ()) -> str:
    """Return CLUSTER BY (...) or '' when the table declares no clustering."""
    if not clustered_by:
        return ""
    quoted_columns = ", ".join(backtick(column_name) for column_name in clustered_by)
    return f"CLUSTER BY ({quoted_columns})"
