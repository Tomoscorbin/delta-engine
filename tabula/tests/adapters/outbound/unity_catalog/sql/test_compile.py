import re

from tabula.adapters.outbound.unity_catalog.sql.compile import compile_plan
from tabula.domain.model.actions import ActionPlan, CreateTable, AddColumn, DropColumn
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType


import pytest

from tabula.adapters.outbound.unity_catalog.sql.compile import compile_plan
from tabula.domain.model.actions import ActionPlan, CreateTable, AddColumn
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType


def qn(
    c="Cat", 
    s="Sch", 
    n="Tbl",
) -> QualifiedName:
    return QualifiedName(c, s, n)


def test_table_identifiers_are_backtick_quoted_and_backticks_escaped():
    plan = ActionPlan(
        qualified_name=qn("cat`alog", "sch`ema", "na`me"),   # backticks in parts to test escaping
        actions=(CreateTable(columns=(Column("c", DataType("int")),)),),
    )
    (sql,) = compile_plan(plan)
    # Escaped backticks appear doubled within backticks
    assert sql.startswith("CREATE TABLE IF NOT EXISTS `cat``alog`.`sch``ema`.`na``me`")


def test_column_identifier_is_quoted_and_backticks_escaped():
    plan = ActionPlan(
        qualified_name=QualifiedName("c", "s", "t"),
        actions=(AddColumn(Column("we`ird", DataType("string"))),),
    )
    (sql,) = compile_plan(plan)
    assert "`we``ird` STRING NOT NULL" in sql


@pytest.mark.parametrize(
    "dtype_name, expected_sql",
    [
        ("string", "STRING"),
        ("boolean", "BOOLEAN"),
        ("int", "INT"),
        ("smallint", "SMALLINT"),
        ("bigint", "BIGINT"),
        ("float", "FLOAT"),
        ("double", "DOUBLE"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMP"),
    ],
)
def test_scalar_type_mapping(dtype_name: str, expected_sql: str):
    plan = ActionPlan(
        qualified_name=QualifiedName("c", "s", "t"),
        actions=(AddColumn(Column("x", DataType(dtype_name))),),
    )
    (sql,) = compile_plan(plan)
    assert f"`x` {expected_sql} NOT NULL" in sql


def test_varchar_is_string_with_length_comment():
    plan = ActionPlan(
        qualified_name=QualifiedName("c", "s", "t"),
        actions=(AddColumn(Column("v", DataType("varchar", (255,)))),),
    )
    (sql,) = compile_plan(plan)
    assert "`v` STRING /* varchar(255) */ NOT NULL" in sql


def test_unknown_type_raises_clear_error():
    plan = ActionPlan(
        qualified_name=QualifiedName("c", "s", "t"),
        actions=(AddColumn(Column("z", DataType("unknown_type"))),),
    )
    with pytest.raises(ValueError) as exc:
        _ = compile_plan(plan)
    assert "Unsupported data type" in str(exc.value)


def test_create_table_always_idempotent_and_not_null_and_quoted():
    plan = ActionPlan(
        qualified_name=qn(),
        actions=(
            CreateTable(
                columns=(
                    Column("id", DataType("int"), is_nullable=False),
                    Column("name", DataType("string"), is_nullable=True),  # should still become NOT NULL by policy
                )
            ),
        ),
    )

    (sql,) = compile_plan(plan)
    # Idempotent
    assert sql.startswith("CREATE TABLE IF NOT EXISTS ")
    # Fully-qualified and backtick quoted
    assert re.search(r"^CREATE TABLE IF NOT EXISTS\s+`cat`\.`sch`\.`tbl`\s*\(", sql)
    # NOT NULL always present
    assert re.search(r"`id`\s+INT\s+NOT NULL", sql)
    assert re.search(r"`name`\s+STRING\s+NOT NULL", sql)
    


def test_add_column_is_single_column_with_if_not_exists_and_not_null():
    plan = ActionPlan(
        qualified_name=qn(),
        actions=(AddColumn(Column("new_col", DataType("double"), is_nullable=True)),),
    )
    (sql,) = compile_plan(plan)
    assert sql == "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN IF NOT EXISTS `new_col` DOUBLE NOT NULL"


def test_drop_column_uses_if_exists_and_quotes_column():
    plan = ActionPlan(
        qualified_name=qn(),
        actions=(DropColumn("ToDrop"),),
    )
    (sql,) = compile_plan(plan)
    assert sql == "ALTER TABLE `cat`.`sch`.`tbl` DROP COLUMN IF EXISTS `todrop`"
