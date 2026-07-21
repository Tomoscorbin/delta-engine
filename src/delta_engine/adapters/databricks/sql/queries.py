"""
Pure SQL query builders for reading Databricks catalog state.

The read-path counterpart of :mod:`compile`: each function renders one
metadata query as text, with identifier quoting and literal escaping handled
here so the reader never assembles SQL inline. Builders are pure — no
SparkSession, no I/O — so query structure is pinned by golden tests.

The primary read is ``DESCRIBE TABLE EXTENDED … AS JSON`` (``describe_json_query``);
the information_schema queries here read the constraint and tag metadata as
structured rows instead of parsing the describe document — Unity Catalog tags,
this table's own primary and foreign keys, and inbound foreign keys — and are
Unity Catalog only.
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
    comment, partition and clustering columns, and table properties in a single
    JSON document. ``AS JSON`` requires ``EXTENDED``. Requires DBR 16.2+; older
    runtimes surface as ``ReadError``. Key constraints and tags are read from
    information_schema instead of this document.
    """
    return f"DESCRIBE TABLE EXTENDED {backtick_qualified_name(qualified_name)} AS JSON"


def schema_exists_query(qualified_name: QualifiedName) -> str:
    """
    Render the information_schema probe for the table's parent schema.

    Returns one row when the schema exists and no rows when it does not.
    Resolving ``<catalog>.information_schema`` requires the catalog itself to
    exist, so a missing catalog fails this query rather than returning no rows.
    """
    catalog = backtick(qualified_name.catalog)
    return (
        f"SELECT schema_name"
        f" FROM {catalog}.information_schema.schemata"
        f" WHERE schema_name = {quote_literal(qualified_name.schema)}"
    )


def primary_key_query(qualified_name: QualifiedName) -> str:
    """
    Render the information_schema query for a table's primary key.

    Returns one row per key column — ``constraint_name`` and ``column_name`` —
    ordered by the column's position in the key, or no rows when the table has
    no primary key. A table has at most one primary key, so every row carries
    the same constraint name.
    """
    catalog = backtick(qualified_name.catalog)
    return (
        f"SELECT tc.constraint_name, kcu.column_name"
        f" FROM {catalog}.information_schema.table_constraints AS tc"
        f" JOIN {catalog}.information_schema.key_column_usage AS kcu"
        f" ON tc.constraint_catalog = kcu.constraint_catalog"
        f" AND tc.constraint_schema = kcu.constraint_schema"
        f" AND tc.constraint_name = kcu.constraint_name"
        f" WHERE tc.table_schema = {quote_literal(qualified_name.schema)}"
        f" AND tc.table_name = {quote_literal(qualified_name.name)}"
        f" AND tc.constraint_type = 'PRIMARY KEY'"
        f" ORDER BY kcu.ordinal_position"
    )


def foreign_keys_query(qualified_name: QualifiedName) -> str:
    """
    Render the information_schema query for the foreign keys this table owns.

    Returns one row per foreign-key column: the constraint name, the local
    column, the referenced table (three parts), and the referenced column. A
    composite key yields one row per column pair, ordered by the local column's
    position. Local and referenced columns are aligned through
    ``position_in_unique_constraint`` — the local column's position in the
    parent key — so a key that lists its parent's columns in a different order
    still pairs correctly, rather than relying on both sides sharing an order.

    information_schema is per-catalog, so a foreign key whose parent table lives
    in another catalog is invisible here — the same per-catalog limit the
    inbound query carries.
    """
    catalog = backtick(qualified_name.catalog)
    return (
        f"SELECT fk.constraint_name,"
        f" fk.column_name AS local_column,"
        f" parent_table.table_catalog AS referenced_catalog,"
        f" parent_table.table_schema AS referenced_schema,"
        f" parent_table.table_name AS referenced_table,"
        f" parent_key.column_name AS referenced_column"
        f" FROM {catalog}.information_schema.referential_constraints AS rc"
        f" JOIN {catalog}.information_schema.key_column_usage AS fk"
        f" ON rc.constraint_catalog = fk.constraint_catalog"
        f" AND rc.constraint_schema = fk.constraint_schema"
        f" AND rc.constraint_name = fk.constraint_name"
        f" JOIN {catalog}.information_schema.key_column_usage AS parent_key"
        f" ON rc.unique_constraint_catalog = parent_key.constraint_catalog"
        f" AND rc.unique_constraint_schema = parent_key.constraint_schema"
        f" AND rc.unique_constraint_name = parent_key.constraint_name"
        f" AND fk.position_in_unique_constraint = parent_key.ordinal_position"
        f" JOIN {catalog}.information_schema.table_constraints AS parent_table"
        f" ON rc.unique_constraint_catalog = parent_table.constraint_catalog"
        f" AND rc.unique_constraint_schema = parent_table.constraint_schema"
        f" AND rc.unique_constraint_name = parent_table.constraint_name"
        f" WHERE fk.table_schema = {quote_literal(qualified_name.schema)}"
        f" AND fk.table_name = {quote_literal(qualified_name.name)}"
        f" ORDER BY fk.constraint_name, fk.ordinal_position"
    )


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
