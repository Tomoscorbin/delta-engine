from hypothesis import given, strategies as st
import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.executor import (
    DatabricksExecutor,
    _execute_statements,
    _sql_preview,
)
from delta_engine.adapters.databricks.sql import compile_plan
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


def _apply(spark, qualified_name: QualifiedName, plan: ActionPlan):
    """Compile then execute, the same two-stage flow the engine drives."""
    executor = DatabricksExecutor(spark)
    return executor.execute(executor.compile(qualified_name, plan))


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

    # When compiling and executing the plan
    summary = _apply(spark, _dummy_qualified_name(), plan)

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
    # Given statements where the second one fails
    statements = ("SELECT 1", "SELECT * FROM __nope__")

    # When executing them
    summary = _execute_statements(_FakeSpark(), statements)

    # Then success and failure are mapped to execution results
    results = summary.results

    assert isinstance(results[0], ExecutionSucceeded)
    assert isinstance(results[1], ExecutionFailed)
    assert results[0].statement_index == 0
    assert results[1].failure.statement_index == 1


def test_execute_failure_records_exception_details_and_sql_preview():
    # Given a failing statement
    statements = ("SELECT * FROM __nope__",)

    # When executing it
    summary = _execute_statements(_FakeSpark(), statements)

    # Then useful debugging details are captured
    [result] = summary.results

    assert isinstance(result, ExecutionFailed)
    assert result.failure.statement_index == 0
    assert result.failure.exception_type == "Exception"
    assert "boom: table not found" in result.failure.message
    assert result.failure.statement_preview == "SELECT * FROM __nope__"


def test_execute_stops_at_first_failure_to_avoid_half_migrating():
    # Given three statements whose middle one fails
    spark = _FakeSpark()
    statements = ("SELECT 1", "SELECT * FROM __nope__", "SELECT 2")

    # When executing them
    summary = _execute_statements(spark, statements)

    # Then the third statement is never attempted
    assert spark.executed == ["SELECT 1", "SELECT * FROM __nope__"]

    results = summary.results
    assert [type(result) for result in results] == [
        ExecutionSucceeded,
        ExecutionFailed,
    ]
    assert results[0].statement_index == 0
    assert results[1].failure.statement_index == 1


def test_execute_returns_empty_summary_for_empty_plan():
    # Given an empty plan
    plan = ActionPlan(actions=())
    spark = _FakeSpark()

    # When compiling and executing the plan
    summary = _apply(spark, _dummy_qualified_name(), plan)

    # Then nothing ran and the summary is non-failing
    assert spark.executed == []
    assert summary.results == ()
    assert summary.failed is False


# ----------- _sql_preview: bounded single-line statement previews for results/logs


def test_sql_preview_single_line_normalization_and_no_truncation():
    sql = " \nSELECT   *\nFROM  foo\tWHERE  a = 1  \n"
    assert _sql_preview(sql, max_chars=10_000) == "SELECT * FROM foo WHERE a = 1"


def test_sql_preview_truncates_and_appends_unicode_ellipsis():
    sql = "SELECT " + "x" * 300 + " FROM t"
    out = _sql_preview(sql, max_chars=50)
    assert out.endswith("…")
    assert len(out) > 50  # because the ellipsis is appended after slicing
    assert out.startswith("SELECT ")


@pytest.mark.parametrize(
    ("length", "truncated"),
    [
        (9, False),  # below the limit: unchanged
        (10, False),  # exactly at the limit: unchanged (the boundary that pins <=)
        (11, True),  # one over: truncated to max_chars + ellipsis
    ],
    ids=["below", "at-limit", "over"],
)
def test_sql_preview_truncates_only_beyond_max_chars(length: int, truncated: bool):
    # Given a single-line SQL string of a known length around max_chars=10
    sql = "x" * length

    # When previewing it with max_chars=10
    out = _sql_preview(sql, max_chars=10)

    # Then it is left intact at or below the limit, and truncated only beyond it
    if truncated:
        assert out == "x" * 10 + "…"
    else:
        assert out == sql


@given(st.text(), st.integers(min_value=1, max_value=500))
def test_sql_preview_single_line_output_never_contains_newline(sql: str, max_chars: int):
    # Given: any SQL string and any max_chars
    result = _sql_preview(sql, max_chars=max_chars)
    # Then: the output never contains a newline regardless of input content
    assert "\n" not in result


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
    summary = _apply(spark, desired.qualified_name, plan)

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
    summary = _apply(spark, qualified_name, plan)

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
    summary = _apply(spark, qualified_name, plan)

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
    summary = _apply(spark, qualified_name, plan)

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
    summary = _apply(spark, qualified_name, plan)

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
    summary = _apply(spark, qualified_name, plan)

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
    summary = _apply(spark, qualified_name, plan)

    # Then the column becomes nullable
    assert summary.failed is False
    assert _get_field(spark, full_table_name, "id").nullable is True


def test_compile_returns_the_statements_execute_would_run():
    # Given a plan with one action
    qualified_name = QualifiedName("cat", "schema", "tbl")
    plan = ActionPlan((SetTableComment(comment="hello"),))
    executor = DatabricksExecutor(spark=None)  # type: ignore[arg-type]  # compile never touches the session

    # When compiling without executing
    statements = executor.compile(qualified_name, plan)

    # Then the statements match the SQL compiler's output, in plan order
    assert statements == compile_plan(qualified_name, plan)
    assert len(statements) == 1
    assert "COMMENT" in statements[0].upper()


def test_compile_of_empty_plan_returns_no_statements():
    executor = DatabricksExecutor(spark=None)  # type: ignore[arg-type]
    assert executor.compile(QualifiedName("cat", "schema", "tbl"), ActionPlan()) == ()
