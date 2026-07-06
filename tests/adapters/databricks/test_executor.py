import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.executor import DatabricksExecutor, _execute_statements
from delta_engine.application.ports import ExecutionFailed, ExecutionSucceeded
from delta_engine.domain.model import Column, DesiredTable, QualifiedName
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
)
from tests.config import TEST_CATALOG

# ----------- Test helpers


def _dummy_qualified_name() -> QualifiedName:
    return QualifiedName("cat", "sch", "tbl")


class _FakeSpark:
    """Minimal Spark stand-in that records SQL and fails on demand."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def sql(self, statement: str):
        self.executed.append(statement)

        if "__nope__" in statement:
            raise Exception("boom: table not found")

        return None


def _get_table_props(spark, full_table_name: str) -> dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {full_table_name}").collect()
    return {row["key"]: row["value"] for row in rows}


def _get_table_comment(spark, full_table_name: str) -> str | None:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").collect()

    for row in rows:
        if row.col_name == "Comment":
            return row.data_type

    return None


def _get_field(spark, full_table_name: str, column_name: str):
    return next(
        field for field in spark.table(full_table_name).schema.fields if field.name == column_name
    )


# ----------- Tests against a fake Spark (fast, run every time)


def test_executor_compiles_plan_and_executes_statements_in_order():
    # Given a public executor and a two-action plan
    spark = _FakeSpark()
    plan = ActionPlan(
        actions=(
            AddColumn(Column("age", Integer())),
            DropColumn("legacy"),
        )
    )

    # When executing the plan
    summary = DatabricksExecutor(spark).execute(_dummy_qualified_name(), plan)

    # Then the compiled SQL is sent to Spark in action order
    assert spark.executed == [
        "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT",
        "ALTER TABLE `cat`.`sch`.`tbl` DROP COLUMN `legacy`",
    ]
    assert [type(result) for result in summary.results] == [
        ExecutionSucceeded,
        ExecutionSucceeded,
    ]


def test_execute_maps_success_and_failure_without_leaking_backend_exception():
    # Given a two-action plan and precompiled statements where the second fails
    plan = ActionPlan(
        actions=(
            AddColumn(Column("a", Integer())),
            DropColumn("b"),
        )
    )
    statements = [
        "SELECT 1",
        "SELECT * FROM __nope__",
    ]

    # When executing statements
    summary = _execute_statements(_FakeSpark(), plan, statements)

    # Then success and failure are mapped to execution results
    results = summary.results

    assert [result.action for result in results] == ["AddColumn", "DropColumn"]
    assert isinstance(results[0], ExecutionSucceeded)
    assert isinstance(results[1], ExecutionFailed)
    assert results[0].action_index == 0
    assert results[1].failure.action_index == 1


def test_execute_failure_records_exception_details_and_sql_preview():
    # Given a failing statement
    plan = ActionPlan(actions=(DropColumn("legacy"),))
    statements = ["SELECT * FROM __nope__"]

    # When executing it
    summary = _execute_statements(_FakeSpark(), plan, statements)

    # Then useful debugging details are captured
    [result] = summary.results

    assert isinstance(result, ExecutionFailed)
    assert result.action == "DropColumn"
    assert result.failure.action_index == 0
    assert result.failure.exception_type == "Exception"
    assert "boom: table not found" in result.failure.message
    assert result.failure.statement_preview == "SELECT * FROM __nope__"


def test_execute_stops_at_first_failure_to_avoid_half_migrating():
    # Given a three-action plan whose middle statement fails
    spark = _FakeSpark()
    plan = ActionPlan(
        actions=(
            AddColumn(Column("a", Integer())),
            DropColumn("b"),
            AddColumn(Column("c", Integer())),
        )
    )
    statements = [
        "SELECT 1",
        "SELECT * FROM __nope__",
        "SELECT 2",
    ]

    # When executing statements
    summary = _execute_statements(spark, plan, statements)

    # Then the third statement is never attempted
    assert spark.executed == statements[:2]

    results = summary.results
    assert [type(result) for result in results] == [
        ExecutionSucceeded,
        ExecutionFailed,
    ]
    assert results[0].action_index == 0
    assert results[1].failure.action_index == 1


def test_execute_returns_empty_summary_for_empty_plan():
    # Given an empty plan
    plan = ActionPlan(actions=())
    spark = _FakeSpark()

    # When executing the plan
    summary = DatabricksExecutor(spark).execute(_dummy_qualified_name(), plan)

    # Then nothing ran and the summary is non-failing
    assert spark.executed == []
    assert summary.results == ()
    assert summary.failed is False


def test_execute_fails_loudly_when_plan_and_statement_lengths_differ():
    # Given a compiler bug produced fewer statements than actions
    plan = ActionPlan(
        actions=(
            AddColumn(Column("a", Integer())),
            DropColumn("b"),
        )
    )
    statements = ["SELECT 1"]

    # When / Then the strict zip guard raises rather than silently truncating
    with pytest.raises(ValueError):
        _execute_statements(_FakeSpark(), plan, statements)


# ----------- Tests against real local Spark/Delta (auto-marked local_e2e via the
# spark fixture; see tests/conftest.py:pytest_collection_modifyitems)


def test_create_table_action_creates_table_with_correct_schema(spark, temp_schema):
    # Given a desired table to be created in an empty schema
    desired = DesiredTable(
        qualified_name=QualifiedName(TEST_CATALOG, temp_schema, "customers"),
        columns=(Column(name="id", data_type=Integer()),),
    )
    plan = ActionPlan(actions=(CreateTable(table=desired),))

    # When applying the plan
    summary = DatabricksExecutor(spark).execute(desired.qualified_name, plan)

    # Then the table exists and its schema matches exactly
    assert summary.failed is False
    assert spark.catalog.tableExists(str(desired.qualified_name))

    actual_schema = spark.table(str(desired.qualified_name)).schema
    expected_schema = T.StructType(
        [
            T.StructField(
                "id",
                T.IntegerType(),
                nullable=True,
            )
        ]
    )
    assert actual_schema == expected_schema


def test_add_column_action_adds_column_to_existing_table(spark, make_temp_table):
    # Given an existing Delta table with one column
    full_table_name = make_temp_table("add_col", "id INT NOT NULL")
    qualified_name = QualifiedName(*full_table_name.split("."))
    plan = ActionPlan(
        actions=(
            AddColumn(
                column=Column(
                    name="age",
                    data_type=Integer(),
                )
            ),
        )
    )

    # When applying the plan
    summary = DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the new column exists with the expected type
    assert summary.failed is False
    age_field = _get_field(spark, full_table_name, "age")
    assert age_field.dataType.simpleString() == "int"


def test_drop_column_action_removes_column_from_existing_table(spark, make_temp_table):
    # Given an existing Delta table with column mapping enabled
    full_table_name = make_temp_table(
        "drop_col",
        "id INT NOT NULL, to_remove STRING",
        tblprops={"delta.columnMapping.mode": "name"},
    )
    qualified_name = QualifiedName(*full_table_name.split("."))
    plan = ActionPlan(actions=(DropColumn(column_name="to_remove"),))

    # When applying the plan
    summary = DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the column no longer exists
    assert summary.failed is False
    assert "to_remove" not in spark.table(full_table_name).columns


def test_set_property_action_sets_table_property(spark, make_temp_table):
    # Given an existing Delta table
    full_table_name = make_temp_table("set_prop", "id INT NOT NULL")
    qualified_name = QualifiedName(*full_table_name.split("."))
    property_name = "engine.test.setproperty"
    plan = ActionPlan(
        actions=(
            SetProperty(
                name=property_name,
                value="yes",
            ),
        )
    )

    # When applying the plan
    summary = DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the property exists with the expected value
    assert summary.failed is False
    assert _get_table_props(spark, full_table_name).get(property_name) == "yes"


def test_set_column_comment_sets_comment_on_column(spark, make_temp_table):
    # Given an existing Delta table with a name column
    full_table_name = make_temp_table("col_comment", "id INT NOT NULL, name STRING")
    qualified_name = QualifiedName(*full_table_name.split("."))
    plan = ActionPlan(
        actions=(
            SetColumnComment(
                column_name="name",
                comment="customer name",
            ),
        )
    )

    # When applying the plan
    summary = DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the column metadata contains the new comment
    assert summary.failed is False
    field = _get_field(spark, full_table_name, "name")
    assert dict(field.metadata).get("comment") == "customer name"


def test_set_table_comment_sets_comment_on_table(spark, make_temp_table):
    # Given an existing Delta table
    full_table_name = make_temp_table("tbl_comment", "id INT NOT NULL")
    qualified_name = QualifiedName(*full_table_name.split("."))
    plan = ActionPlan(actions=(SetTableComment(comment="staging table"),))

    # When applying the plan
    summary = DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the table comment is set
    assert summary.failed is False
    assert _get_table_comment(spark, full_table_name) == "staging table"


def test_set_column_nullability_sets_nullable(spark, make_temp_table):
    # Given an existing table with a NOT NULL id column
    full_table_name = make_temp_table("nullability", "id INT NOT NULL, name STRING")
    qualified_name = QualifiedName(*full_table_name.split("."))
    plan = ActionPlan(
        actions=(
            SetColumnNullability(
                column_name="id",
                nullable=True,
            ),
        )
    )

    # When applying the plan
    summary = DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the column becomes nullable
    assert summary.failed is False
    assert _get_field(spark, full_table_name, "id").nullable is True
