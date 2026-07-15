"""
Pure SQL query builders for reading Databricks catalog state.

The read-path counterpart of :mod:`compile`: each function renders one
metadata query as text, with identifier quoting and literal escaping handled
here so the reader never assembles SQL inline. Builders are pure — no
SparkSession, no I/O — so query structure is pinned by golden tests.

The primary read is ``DESCRIBE TABLE EXTENDED … AS JSON`` (``describe_json_query``);
the information_schema queries here supply only what that JSON omits — Unity
Catalog tags and inbound foreign keys — and are Unity Catalog only.
"""

from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.domain.model import QualifiedName


def describe_json_query(qualified_name: QualifiedName) -> str:
    """
    Render ``DESCRIBE TABLE EXTENDED <table> AS JSON``.

    The one primary read: it returns columns (structured types), the table
    comment, partition and clustering columns, table properties, and the
    ``table_constraints`` string in a single JSON document. ``AS JSON``
    requires ``EXTENDED``. Requires DBR 16.2+ (constraints: 17.3+ or a SQL
    warehouse); older runtimes surface as ``ReadFailed``.
    """
    return f"DESCRIBE TABLE EXTENDED {backtick_qualified_name(qualified_name)} AS JSON"


def table_tags_query(qualified_name: QualifiedName) -> str:
    """Render the information_schema query for a table's Unity Catalog tags."""
    catalog = backtick(qualified_name.catalog)
    return (
        f"SELECT tag_name, tag_value"
        f" FROM {catalog}.information_schema.table_tags"
        f" WHERE schema_name = {quote_literal(qualified_name.schema)}"
        f" AND table_name = {quote_literal(qualified_name.name)}"
    )


def column_tags_query(qualified_name: QualifiedName) -> str:
    """
    Render the information_schema query for all column tags of one table.

    One query covers all of the table's columns, avoiding a per-column round-trip.
    """
    catalog = backtick(qualified_name.catalog)
    return (
        f"SELECT column_name, tag_name, tag_value"
        f" FROM {catalog}.information_schema.column_tags"
        f" WHERE schema_name = {quote_literal(qualified_name.schema)}"
        f" AND table_name = {quote_literal(qualified_name.name)}"
    )


def referencing_foreign_keys_query(qualified_name: QualifiedName) -> str:
    """
    Render the information_schema query for foreign keys referencing this table.

    Finds foreign keys owned by *other* tables whose parent key lives on this
    table, joining table_constraints twice — once to locate the parent key's
    table (the WHERE filter) and once to name the referencing constraint's own
    table. Column detail is not needed; validation only names what blocks a
    primary-key change.

    The parent constraint is filtered to the primary key: a foreign key may
    also reference a UNIQUE constraint (DBR 18.2+), and such a key does not
    block ``DROP PRIMARY KEY`` — RESTRICT only rejects the drop for keys that
    depend on the primary key itself.

    information_schema is per-catalog, so a foreign key owned by a table in a
    different catalog is invisible here; such a drop still fails at execution.
    """
    catalog = backtick(qualified_name.catalog)
    return (
        f"SELECT rc.constraint_name,"
        f" fk_tables.table_catalog AS referencing_catalog,"
        f" fk_tables.table_schema AS referencing_schema,"
        f" fk_tables.table_name AS referencing_table"
        f" FROM {catalog}.information_schema.referential_constraints AS rc"
        f" JOIN {catalog}.information_schema.table_constraints AS pk_tables"
        f" ON rc.unique_constraint_catalog = pk_tables.constraint_catalog"
        f" AND rc.unique_constraint_schema = pk_tables.constraint_schema"
        f" AND rc.unique_constraint_name = pk_tables.constraint_name"
        f" JOIN {catalog}.information_schema.table_constraints AS fk_tables"
        f" ON rc.constraint_catalog = fk_tables.constraint_catalog"
        f" AND rc.constraint_schema = fk_tables.constraint_schema"
        f" AND rc.constraint_name = fk_tables.constraint_name"
        f" WHERE pk_tables.table_schema = {quote_literal(qualified_name.schema)}"
        f" AND pk_tables.table_name = {quote_literal(qualified_name.name)}"
        f" AND pk_tables.constraint_type = 'PRIMARY KEY'"
        f" ORDER BY fk_tables.table_catalog, fk_tables.table_schema,"
        f" fk_tables.table_name, rc.constraint_name"
    )
