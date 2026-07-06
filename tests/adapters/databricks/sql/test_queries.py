"""Golden and structural tests for the pure information_schema query builders."""

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_detail_query,
    foreign_keys_query,
    primary_key_query,
    table_tags_query,
)
from delta_engine.domain.model import QualifiedName

QN = QualifiedName("cat", "sch", "tbl")


def test_describe_detail_query_backticks_the_table_name():
    assert describe_detail_query(QN) == "DESCRIBE DETAIL `cat`.`sch`.`tbl`"


def test_primary_key_query_golden():
    assert primary_key_query(QN) == (
        "SELECT table_constraints_info.constraint_name,"
        " constraint_columns.column_name"
        " FROM `cat`.information_schema.constraint_column_usage"
        " AS constraint_columns"
        " JOIN `cat`.information_schema.table_constraints"
        " AS table_constraints_info"
        " USING (constraint_catalog, constraint_schema, constraint_name)"
        " WHERE constraint_columns.table_schema = 'sch'"
        " AND constraint_columns.table_name = 'tbl'"
        " AND table_constraints_info.constraint_type = 'PRIMARY KEY'"
    )


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


def test_foreign_keys_query_golden():
    assert foreign_keys_query(QN) == (
        "SELECT rc.constraint_name,"
        " kcu.column_name AS local_column,"
        " kcu.ordinal_position,"
        " kcu.position_in_unique_constraint,"
        " pk.table_catalog AS ref_catalog,"
        " pk.table_schema AS ref_schema,"
        " pk.table_name AS ref_table,"
        " pk.column_name AS ref_column"
        " FROM `cat`.information_schema.referential_constraints AS rc"
        " JOIN `cat`.information_schema.key_column_usage AS kcu"
        " USING (constraint_catalog, constraint_schema, constraint_name)"
        " JOIN `cat`.information_schema.key_column_usage AS pk"
        " ON rc.unique_constraint_catalog = pk.constraint_catalog"
        " AND rc.unique_constraint_schema = pk.constraint_schema"
        " AND rc.unique_constraint_name = pk.constraint_name"
        " AND kcu.position_in_unique_constraint = pk.ordinal_position"
        " WHERE kcu.table_schema = 'sch'"
        " AND kcu.table_name = 'tbl'"
        " ORDER BY rc.constraint_name, kcu.ordinal_position"
    )


def test_foreign_keys_query_correlates_referenced_columns_by_parent_key_position():
    # The FK query must align referenced columns via position_in_unique_constraint
    # (constraint_column_usage has no ordinal, so it cannot align composite keys).
    query = foreign_keys_query(QN)
    assert "position_in_unique_constraint" in query
    assert "constraint_column_usage" not in query


def test_queries_escape_identifiers_and_literals():
    # Backticked identifier parts double embedded backticks; string literals
    # double embedded single quotes.
    qn = QualifiedName("ca`t", "sc'h", "tbl")
    assert "`ca``t`" in primary_key_query(qn)
    assert "'sc''h'" in primary_key_query(qn)


def test_information_schema_probe_query_golden():
    from delta_engine.adapters.databricks.sql import information_schema_probe_query

    assert information_schema_probe_query("cat") == (
        "SELECT 1 FROM `cat`.information_schema.schemata WHERE 1 = 0"
    )
