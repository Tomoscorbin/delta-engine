"""Golden and structural tests for the pure information_schema query builders."""

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_detail_query,
    describe_json_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    schema_exists_query,
    table_tags_query,
)
from delta_engine.domain.model import QualifiedName

QN = QualifiedName("cat", "sch", "tbl")


def test_describe_json_query_is_extended_and_backticked():
    assert describe_json_query(QN) == "DESCRIBE TABLE EXTENDED `cat`.`sch`.`tbl` AS JSON"


def test_describe_detail_query_is_backticked():
    assert describe_detail_query(QN) == "DESCRIBE DETAIL `cat`.`sch`.`tbl`"


def test_schema_exists_query_golden():
    assert schema_exists_query(QN) == (
        "SELECT schema_name FROM `cat`.information_schema.schemata WHERE schema_name = 'sch'"
    )


def test_primary_key_query_golden():
    assert primary_key_query(QN) == (
        "SELECT tc.constraint_name, kcu.column_name"
        " FROM `cat`.information_schema.table_constraints AS tc"
        " JOIN `cat`.information_schema.key_column_usage AS kcu"
        " ON tc.constraint_catalog = kcu.constraint_catalog"
        " AND tc.constraint_schema = kcu.constraint_schema"
        " AND tc.constraint_name = kcu.constraint_name"
        " WHERE tc.table_schema = 'sch'"
        " AND tc.table_name = 'tbl'"
        " AND tc.constraint_type = 'PRIMARY KEY'"
        " ORDER BY kcu.ordinal_position"
    )


def test_foreign_keys_query_golden():
    assert foreign_keys_query(QN) == (
        "SELECT fk.constraint_name,"
        " fk.column_name AS local_column,"
        " parent_table.table_catalog AS referenced_catalog,"
        " parent_table.table_schema AS referenced_schema,"
        " parent_table.table_name AS referenced_table,"
        " parent_key.column_name AS referenced_column"
        " FROM `cat`.information_schema.referential_constraints AS rc"
        " JOIN `cat`.information_schema.key_column_usage AS fk"
        " ON rc.constraint_catalog = fk.constraint_catalog"
        " AND rc.constraint_schema = fk.constraint_schema"
        " AND rc.constraint_name = fk.constraint_name"
        " JOIN `cat`.information_schema.key_column_usage AS parent_key"
        " ON rc.unique_constraint_catalog = parent_key.constraint_catalog"
        " AND rc.unique_constraint_schema = parent_key.constraint_schema"
        " AND rc.unique_constraint_name = parent_key.constraint_name"
        " AND fk.position_in_unique_constraint = parent_key.ordinal_position"
        " JOIN `cat`.information_schema.table_constraints AS parent_table"
        " ON rc.unique_constraint_catalog = parent_table.constraint_catalog"
        " AND rc.unique_constraint_schema = parent_table.constraint_schema"
        " AND rc.unique_constraint_name = parent_table.constraint_name"
        " WHERE fk.table_schema = 'sch'"
        " AND fk.table_name = 'tbl'"
        " ORDER BY fk.constraint_name, fk.ordinal_position"
    )


def test_foreign_keys_query_pairs_columns_by_position_in_unique_constraint():
    # A composite foreign key's local and referenced columns are aligned by the
    # local column's position in the parent key, so a key that references its
    # parent's columns in a different order than they are declared still pairs
    # correctly instead of relying on both sides sharing an order.
    assert "fk.position_in_unique_constraint = parent_key.ordinal_position" in foreign_keys_query(
        QN
    )


def test_referencing_foreign_keys_query_golden():
    assert referencing_foreign_keys_query(QN) == (
        "SELECT rc.constraint_name,"
        " fk_tables.table_catalog AS referencing_catalog,"
        " fk_tables.table_schema AS referencing_schema,"
        " fk_tables.table_name AS referencing_table"
        " FROM `cat`.information_schema.referential_constraints AS rc"
        " JOIN `cat`.information_schema.table_constraints AS pk_tables"
        " ON rc.unique_constraint_catalog = pk_tables.constraint_catalog"
        " AND rc.unique_constraint_schema = pk_tables.constraint_schema"
        " AND rc.unique_constraint_name = pk_tables.constraint_name"
        " JOIN `cat`.information_schema.table_constraints AS fk_tables"
        " ON rc.constraint_catalog = fk_tables.constraint_catalog"
        " AND rc.constraint_schema = fk_tables.constraint_schema"
        " AND rc.constraint_name = fk_tables.constraint_name"
        " WHERE pk_tables.table_schema = 'sch'"
        " AND pk_tables.table_name = 'tbl'"
        " AND pk_tables.constraint_type = 'PRIMARY KEY'"
        " ORDER BY fk_tables.table_catalog, fk_tables.table_schema,"
        " fk_tables.table_name, rc.constraint_name"
    )


def test_referencing_foreign_keys_query_matches_primary_key_parents_only():
    # A foreign key may reference a UNIQUE constraint (DBR 18.2+); such a key
    # does not block a primary-key drop, so the inbound query must filter the
    # parent constraint to the primary key.
    assert "pk_tables.constraint_type = 'PRIMARY KEY'" in referencing_foreign_keys_query(QN)


def test_table_tags_query_golden():
    assert table_tags_query(QN) == (
        "SELECT tag_name, tag_value"
        " FROM `cat`.information_schema.table_tags"
        " WHERE schema_name = 'sch'"
        " AND table_name = 'tbl'"
    )


def test_column_tags_query_golden():
    assert column_tags_query(QN) == (
        "SELECT column_name, tag_name, tag_value"
        " FROM `cat`.information_schema.column_tags"
        " WHERE schema_name = 'sch'"
        " AND table_name = 'tbl'"
    )


def test_queries_escape_identifiers_and_literals():
    # Backticked identifier parts double embedded backticks; string literals
    # double embedded single quotes.
    qn = QualifiedName("ca`t", "sc'h", "tbl")
    assert "`ca``t`" in referencing_foreign_keys_query(qn)
    assert "'sc''h'" in table_tags_query(qn)
