"""
Independent catalog readers for live SQL Warehouse tests.

These read observed table state straight from information_schema and
DESCRIBE DETAIL so assertions do not depend on the engine's own reader.
Run instructions and the ``live_connection``/``live_tables`` fixtures live
in ``conftest.py``.
"""

from collections.abc import Mapping
import json
import os
from typing import Any

from delta_engine.adapters.databricks.sql.dialect import backtick, quote_literal


def live_catalog() -> str:
    return os.environ["DELTA_ENGINE_E2E_CATALOG"]


def live_schema() -> str:
    return os.environ["DELTA_ENGINE_E2E_SCHEMA"]


def qualified_table(table_name: str) -> str:
    return ".".join(backtick(part) for part in (live_catalog(), live_schema(), table_name))


def fetch_rows(connection, statement: str) -> list[dict[str, Any]]:
    """Execute a catalog query and return rows keyed by case-folded column name."""
    with connection.cursor() as cursor:
        cursor.execute(statement)
        assert cursor.description is not None
        names = tuple(column[0].casefold() for column in cursor.description)
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def execute_sql(connection, statement: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(statement)


def _decode_mapping(value: object) -> dict[str, str]:
    decoded = json.loads(value) if isinstance(value, str) else value
    assert isinstance(decoded, Mapping)
    return {str(key): str(item) for key, item in decoded.items()}


def _decode_strings(value: object) -> tuple[str, ...]:
    decoded = json.loads(value) if isinstance(value, str) else value
    assert isinstance(decoded, (list, tuple))
    return tuple(str(item) for item in decoded)


def table_exists(connection, table_name: str) -> bool:
    rows = fetch_rows(
        connection,
        f"SELECT table_name FROM {backtick(live_catalog())}.information_schema.tables "
        f"WHERE table_schema = {quote_literal(live_schema())} "
        f"AND table_name = {quote_literal(table_name)}",
    )
    return bool(rows)


def read_live_table(connection, table_name: str) -> dict[str, Any]:
    """Read observable physical state independently through the SQL warehouse."""
    catalog = live_catalog()
    schema = live_schema()
    information_schema = f"{backtick(catalog)}.information_schema"
    table_filter = (
        f"table_schema = {quote_literal(schema)} AND table_name = {quote_literal(table_name)}"
    )
    tag_filter = (
        f"schema_name = {quote_literal(schema)} AND table_name = {quote_literal(table_name)}"
    )

    columns = fetch_rows(
        connection,
        "SELECT column_name, full_data_type, is_nullable, comment, partition_index "
        f"FROM {information_schema}.columns WHERE {table_filter} ORDER BY ordinal_position",
    )
    [table] = fetch_rows(
        connection,
        f"SELECT comment, last_altered FROM {information_schema}.tables WHERE {table_filter}",
    )
    [detail] = fetch_rows(connection, f"DESCRIBE DETAIL {qualified_table(table_name)}")
    table_tags = fetch_rows(
        connection,
        f"SELECT tag_name, tag_value FROM {information_schema}.table_tags WHERE {tag_filter}",
    )
    column_tags = fetch_rows(
        connection,
        "SELECT column_name, tag_name, tag_value "
        f"FROM {information_schema}.column_tags WHERE {tag_filter}",
    )
    primary_key = fetch_rows(
        connection,
        "SELECT keys.constraint_name, keys.column_name "
        f"FROM {information_schema}.key_column_usage AS keys "
        f"JOIN {information_schema}.table_constraints AS constraints "
        "USING (constraint_catalog, constraint_schema, constraint_name) "
        f"WHERE keys.table_schema = {quote_literal(schema)} "
        f"AND keys.table_name = {quote_literal(table_name)} "
        "AND constraints.constraint_type = 'PRIMARY KEY' "
        "ORDER BY keys.ordinal_position",
    )
    foreign_keys = fetch_rows(
        connection,
        "SELECT rc.constraint_name, child.column_name AS local_column, "
        "parent.table_name AS referenced_table, parent.column_name AS referenced_column "
        f"FROM {information_schema}.referential_constraints AS rc "
        f"JOIN {information_schema}.key_column_usage AS child "
        "USING (constraint_catalog, constraint_schema, constraint_name) "
        f"JOIN {information_schema}.key_column_usage AS parent "
        "ON rc.unique_constraint_catalog = parent.constraint_catalog "
        "AND rc.unique_constraint_schema = parent.constraint_schema "
        "AND rc.unique_constraint_name = parent.constraint_name "
        "AND child.position_in_unique_constraint = parent.ordinal_position "
        f"WHERE child.table_schema = {quote_literal(schema)} "
        f"AND child.table_name = {quote_literal(table_name)} "
        "ORDER BY rc.constraint_name, child.ordinal_position",
    )

    return {
        "columns": columns,
        "comment": table["comment"],
        "last_altered": table["last_altered"],
        "format": detail["format"],
        "clustering": _decode_strings(detail.get("clusteringcolumns", [])),
        "partitioning": _decode_strings(detail["partitioncolumns"]),
        "properties": _decode_mapping(detail["properties"]),
        "table_tags": {row["tag_name"]: row["tag_value"] for row in table_tags},
        "column_tags": {
            (row["column_name"], row["tag_name"]): row["tag_value"] for row in column_tags
        },
        "primary_key": tuple(row["column_name"] for row in primary_key),
        "primary_key_name": primary_key[0]["constraint_name"] if primary_key else None,
        "foreign_keys": tuple(
            (
                row["constraint_name"],
                row["local_column"],
                row["referenced_table"],
                row["referenced_column"],
            )
            for row in foreign_keys
        ),
    }
