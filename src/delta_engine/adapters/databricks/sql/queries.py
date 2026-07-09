"""
Pure SQL query builders for reading Databricks catalog state.

The read-path counterpart of :mod:`compile`: each function renders one
metadata query as text, with identifier quoting and literal escaping handled
here so the reader never assembles SQL inline. Builders are pure — no
SparkSession, no I/O — so query structure is pinned by golden tests.

All information_schema queries are Unity Catalog only; the reader owns the
policy for environments where information_schema does not exist.
"""

from delta_engine.adapters.databricks.sql.dialect import (
    backtick,
    backtick_qualified_name,
    quote_literal,
)
from delta_engine.domain.model import QualifiedName


def describe_detail_query(qualified_name: QualifiedName) -> str:
    """
    Render the DESCRIBE DETAIL statement for a table's properties.

    The name is interpolated into SQL text here, so it must be backtick-quoted
    to stay an identifier (and escape any embedded backtick). This differs
    deliberately from the reader's ``spark.catalog.*`` calls, which take the
    plain ``str()`` form because they parse the dot-separated parts themselves.
    Don't unify the two.

    DESCRIBE DETAIL is load-bearing: its ``properties`` column is
    ``Metadata.configuration`` verbatim — Delta strips protocol keys
    (``delta.minReaderVersion``, ``delta.feature.*``) from it before every
    commit. Never switch to ``SHOW TBLPROPERTIES``, which synthesizes those
    protocol rows into its output at read time.
    """
    return f"DESCRIBE DETAIL {backtick_qualified_name(qualified_name)}"


def primary_key_query(qualified_name: QualifiedName) -> str:
    """Render the information_schema query for a table's primary key columns."""
    catalog = backtick(qualified_name.catalog)
    return (
        f"SELECT table_constraints_info.constraint_name,"
        f" constraint_columns.column_name"
        f" FROM {catalog}.information_schema.constraint_column_usage"
        f" AS constraint_columns"
        f" JOIN {catalog}.information_schema.table_constraints"
        f" AS table_constraints_info"
        f" USING (constraint_catalog, constraint_schema, constraint_name)"
        f" WHERE constraint_columns.table_schema ="
        f" {quote_literal(qualified_name.schema)}"
        f" AND constraint_columns.table_name ="
        f" {quote_literal(qualified_name.name)}"
        f" AND table_constraints_info.constraint_type = 'PRIMARY KEY'"
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


def foreign_keys_query(qualified_name: QualifiedName) -> str:
    """
    Render the information_schema query for a table's foreign keys.

    Reads the FK's local columns from key_column_usage (kcu). For a foreign
    key, kcu also exposes position_in_unique_constraint: the 1-based position
    of each local column within the *parent* key. The referenced columns are
    resolved by reading the parent key's own kcu rows (aliased pk) and
    aligning them to the FK by that position. constraint_column_usage has no
    ordinal, so it cannot align composite keys — hence the self-join.

    Ordered by (constraint_name, ordinal_position) so each constraint's rows
    are contiguous and already in column order; the row mapper relies on this.
    """
    catalog = backtick(qualified_name.catalog)
    return (
        f"SELECT rc.constraint_name,"
        f" kcu.column_name AS local_column,"
        f" kcu.ordinal_position,"
        f" kcu.position_in_unique_constraint,"
        f" pk.table_catalog AS ref_catalog,"
        f" pk.table_schema AS ref_schema,"
        f" pk.table_name AS ref_table,"
        f" pk.column_name AS ref_column"
        f" FROM {catalog}.information_schema.referential_constraints AS rc"
        f" JOIN {catalog}.information_schema.key_column_usage AS kcu"
        f" USING (constraint_catalog, constraint_schema, constraint_name)"
        f" JOIN {catalog}.information_schema.key_column_usage AS pk"
        f" ON rc.unique_constraint_catalog = pk.constraint_catalog"
        f" AND rc.unique_constraint_schema = pk.constraint_schema"
        f" AND rc.unique_constraint_name = pk.constraint_name"
        f" AND kcu.position_in_unique_constraint = pk.ordinal_position"
        f" WHERE kcu.table_schema = {quote_literal(qualified_name.schema)}"
        f" AND kcu.table_name = {quote_literal(qualified_name.name)}"
        f" ORDER BY rc.constraint_name, kcu.ordinal_position"
    )


def referencing_foreign_keys_query(qualified_name: QualifiedName) -> str:
    """
    Render the information_schema query for foreign keys referencing this table.

    The inbound counterpart of :func:`foreign_keys_query`: it finds FKs owned
    by *other* tables whose parent key lives on this table, joining
    table_constraints twice — once to locate the parent key's table (the
    WHERE filter) and once to name the referencing constraint's own table.
    Column detail is not needed; validation only names what blocks a
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


def information_schema_probe_query(catalog: str) -> str:
    """
    Render a cheap query that succeeds exactly where information_schema exists.

    Takes the catalog name (not a table's QualifiedName): availability is a
    per-catalog fact, probed once and cached by the reader. ``WHERE 1 = 0``
    keeps it free — the planner still resolves the view, which is the test.
    """
    return f"SELECT 1 FROM {backtick(catalog)}.information_schema.schemata WHERE 1 = 0"
