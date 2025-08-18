import re

from tabula.adapters.databricks.sql.compile import (
    compile_plan,
    sql_columns_query,
    sql_describe_detail_query,
    sql_empty_probe_query,
)
from tabula.domain.plan.actions import ActionPlan, CreateTable, AddColumn, DropColumn
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.model.qualified_name import QualifiedName


def qn(c="Cat", s="Sch", n="Tbl") -> QualifiedName:
    return QualifiedName(c, s, n)


def test_create_table_is_idempotent_not_null_and_quoted():
    plan = ActionPlan(
        qn(),
        actions=(
            CreateTable(
                columns=(
                    Column("id", DataType("integer"), is_nullable=False),
                    Column("name", DataType("string"), is_nullable=True),
                )
            ),
        ),
    )
    (sql,) = compile_plan(plan)
    assert sql.startswith("CREATE TABLE IF NOT EXISTS ")
    assert re.search(r"^CREATE TABLE IF NOT EXISTS\s+`cat`\.`sch`\.`tbl`\s*\(", sql)
    assert "`id` INT NOT NULL" in sql
    assert "`name` STRING NOT NULL" in sql


def test_add_and_drop_column_are_idempotent():
    plan = ActionPlan(
        qn(),
        actions=(
            AddColumn(Column("new_col", DataType("double"), is_nullable=True)),
            DropColumn("ToDrop"),
        ),
    )
    sql = compile_plan(plan)
    assert sql[0] == "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN IF NOT EXISTS `new_col` DOUBLE NOT NULL"
    assert sql[1] == "ALTER TABLE `cat`.`sch`.`tbl` DROP COLUMN IF EXISTS `todrop`"


def test_sql_columns_query_uses_literals_and_orders_by_position():
    name_with_quote = QualifiedName("Cat", "S'c", "T'bl")
    text = sql_columns_query(name_with_quote)
    # literals are single-quoted with doubled single quotes inside
    assert "table_schema  = 's''c'" in text
    assert "table_name    = 't''bl'" in text
    assert text.strip().endswith("ORDER BY ordinal_position")


def test_describe_detail_and_probe_queries_use_table_ref():
    text1 = sql_describe_detail_query(qn())
    text2 = sql_empty_probe_query(qn())
    assert text1 == "DESCRIBE DETAIL `cat`.`sch`.`tbl`"
    assert text2 == "SELECT 1 FROM `cat`.`sch`.`tbl` LIMIT 1"
