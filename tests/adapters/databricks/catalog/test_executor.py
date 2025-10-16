import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.catalog.executor import DatabricksExecutor
from delta_engine.application.results import ActionStatus
from delta_engine.domain.model import Column, DesiredTable, QualifiedName, TableFormat
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.plan import (
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetColumnNullability,
    SetProperty,
    SetTableComment,
    UnsetProperty,
)
from tests.config import TEST_CATALOG, TEST_SCHEMA

# ----------- Fixtures & test helpers


@pytest.fixture(scope="module")
def test_table(spark):
    """Creates a simple Delta table to mutate during tests, and drops the schema afterwards."""
    catalog = TEST_CATALOG
    schema = TEST_SCHEMA
    full_table_name = f"{catalog}.{schema}.test"

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{schema}")
        spark.sql(
            f"CREATE TABLE {full_table_name}"
            " (id INT NOT NULL, name STRING, column_to_drop STRING)"
            " USING DELTA"
            " TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
        )
        yield QualifiedName(catalog, schema, "test")
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")


def _get_table_props(spark, full_table_name: str) -> dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {full_table_name}").collect()
    return {r["key"]: r["value"] for r in rows}


def _get_table_comment(spark, full_table_name: str) -> str | None:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").collect()
    for r in rows:
        if r.col_name == "Comment":
            return r.data_type
    return None


def _get_field(spark, full_table_name: str, column_name: str):
    return next(f for f in spark.table(full_table_name).schema.fields if f.name == column_name)


def _dummy_target() -> QualifiedName:
    return QualifiedName("cat", "sch", "tbl")


class _FakeSpark:
    """Minimal stand-in for Spark that can 'execute' or fail on demand."""

    def sql(self, statement: str):
        if "__nope__" in statement:
            raise Exception("boom: table not found")
        return None


# ----------- Tests


def test_execute_maps_success_and_failure_no_leakage(caplog):
    # Given a 3-action plan and a compiler that yields OK → FAIL → OK
    plan = ActionPlan(
        target=_dummy_target(),
        actions=(
            AddColumn(Column("a", Integer())),
            DropColumn("b"),
            AddColumn(Column("c", Integer())),
        ),
    )

    def _fake_compiler(_plan):
        return [
            "SELECT 1",  # OK
            "SELECT * FROM __nope__",  # FAIL (FakeSpark will raise)
            "SELECT 2",  # OK
        ]

    # When we execute
    results = DatabricksExecutor(_FakeSpark(), compiler=_fake_compiler).execute(plan)

    # Then statuses are OK, FAILED, OK and action metadata is correct
    assert [r.action for r in results] == ["AddColumn", "DropColumn", "AddColumn"]
    assert [r.action_index for r in results] == [0, 1, 2]
    assert [r.status for r in results] == [
        ActionStatus.OK,
        ActionStatus.FAILED,
        ActionStatus.OK,
    ]
    assert results[0].failure is None
    assert results[1].failure is not None
    assert results[2].failure is None


def test_execute_returns_empty_tuple_for_empty_plan():
    # Given an empty plan
    plan = ActionPlan(target=_dummy_target(), actions=())

    # When we execute the plan
    results = DatabricksExecutor(_FakeSpark()).execute(plan)

    # Then no results are returned
    assert results == ()


def test_createtable_action_creates_table_with_correct_schema(spark, test_table):
    # Given a desired customers table to be created
    desired = DesiredTable(
        qualified_name=QualifiedName(TEST_CATALOG, TEST_SCHEMA, "customers"),
        columns=(Column(name="id", data_type=Integer()),),
        format=TableFormat.DELTA,
    )
    plan = ActionPlan(target=desired.qualified_name, actions=(CreateTable(table=desired),))
    executor = DatabricksExecutor(spark)

    # When we apply the plan (compile → execute)
    executor._apply(plan)

    # Then the table exists and its schema matches exactly
    assert spark.catalog.tableExists(str(desired.qualified_name))
    actual_schema = spark.table(str(desired.qualified_name)).schema
    expected_schema = T.StructType([T.StructField("id", T.IntegerType(), nullable=True)])
    assert actual_schema == expected_schema


def test_addcolumn_action_adds_column_to_existing_table(spark, test_table):
    # Given an existing table (test_table)
    # And a plan that adds a new INT column 'age'
    column_name = "age"
    data_type = Integer()
    actions = (AddColumn(column=Column(name=column_name, data_type=data_type)),)
    plan = ActionPlan(
        target=test_table,
        actions=actions,
    )

    # When we apply the plan (compile → execute)
    DatabricksExecutor(spark)._apply(plan)

    # Then the new column exists
    actual_schema = spark.table(str(test_table)).schema
    assert any(f.name == column_name for f in actual_schema.fields), (
        f"Column '{column_name}' was not created"
    )
    age_field = next(f for f in actual_schema.fields if f.name == "age")
    assert age_field.dataType.simpleString() == "int", (
        f"Column '{column_name}' is not {data_type!s}"
    )


def test_dropcolumn_action_removes_column_from_existing_table(spark, test_table):
    # Given an existing table (test_table)
    # And a plan that drops the 'name' column
    actions = (DropColumn(column_name="column_to_drop"),)
    plan = ActionPlan(
        target=test_table,
        actions=actions,
    )

    # When we apply the plan (compile → execute)
    DatabricksExecutor(spark)._apply(plan)

    # Then the 'name' column no longer exists
    actual_columns = spark.table(str(test_table)).columns
    assert "column_to_drop" not in actual_columns


def test_setproperty_action_sets_table_property(spark, test_table):
    # Given an existing table
    # And a plan that sets a custom property
    prop = "engine.test.setproperty"
    val = "yes"
    plan = ActionPlan(target=test_table, actions=(SetProperty(name=prop, value=val),))

    # When we apply the plan (compile → execute)
    DatabricksExecutor(spark)._apply(plan)

    # Then the property exists with the expected value
    props = _get_table_props(spark, str(test_table))
    assert props.get(prop) == val


def test_unsetproperty_action_removes_existing_property(spark, test_table):
    # Given a table with a property set
    # And a plan that unsets a property
    prop = "delta.columnMapping.mode"
    plan = ActionPlan(target=test_table, actions=(UnsetProperty(name=prop),))

    # When we apply the plan (compile → execute)
    DatabricksExecutor(spark)._apply(plan)

    # Then the property is gone
    props = _get_table_props(spark, str(test_table))
    assert prop not in props


def test_unsetproperty_action_is_idempotent_when_missing(spark, test_table):
    # Given a property is not set
    prop = "engine.test.missing"
    plan = ActionPlan(test_table, (UnsetProperty(name=prop),))

    # When we unset a non-existent property
    DatabricksExecutor(spark)._apply(plan)

    # Then still not present, and no exception was raised
    props = _get_table_props(spark, str(test_table))
    assert prop not in props


def test_setcolumncomment_sets_comment_on_column(spark, test_table):
    # Given an existing Delta table with column 'name'
    column = "name"

    # When we apply a plan to set the column comment
    new_comment = "customer name"
    plan = ActionPlan(
        target=test_table,
        actions=(SetColumnComment(column_name=column, comment=new_comment),),
    )
    DatabricksExecutor(spark)._apply(plan)

    # Then the column metadata contains the new comment
    after_field = next(f for f in spark.table(str(test_table)).schema if f.name == column)
    assert dict(after_field.metadata).get("comment") == new_comment


def test_settablecomment_sets_comment_on_table(spark, test_table):
    # Given an existing Delta table
    # When we set the table comment
    comment = "customers staging table"
    plan = ActionPlan(target=test_table, actions=(SetTableComment(comment=comment),))
    DatabricksExecutor(spark)._apply(plan)

    # Then the comment is set on the table
    after = _get_table_comment(spark, str(test_table))
    assert after == comment


def test_setcolumnnullability_sets_nullable(spark, test_table):
    # Given an existing table with a non-nullable 'id' column
    # When we set NULL on 'id'
    full = str(test_table)
    plan = ActionPlan(test_table, (SetColumnNullability("id", True),))
    DatabricksExecutor(spark)._apply(plan)

    # Then the column becomes NULLABLE
    field = _get_field(spark, full, "id")
    assert field.nullable is True
