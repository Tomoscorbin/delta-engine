"""Golden and structural tests for the pure information_schema query builders."""

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_json_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.domain.model import QualifiedName

QN = QualifiedName("cat", "sch", "tbl")


def test_describe_json_query_is_extended_and_backticked():
    assert describe_json_query(QN) == "DESCRIBE TABLE EXTENDED `cat`.`sch`.`tbl` AS JSON"


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
