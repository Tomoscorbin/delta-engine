import pyspark.sql.types as T

from delta_engine.adapters.databricks.executor import DatabricksExecutor, _execute_statements
from delta_engine.application.results import ExecutionFailed, ExecutionSucceeded
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


def _dummy_qualified_name() -> QualifiedName:
    return QualifiedName("cat", "sch", "tbl")


class _FakeSpark:
    """Minimal stand-in for Spark that records executed statements and fails on demand."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def sql(self, statement: str):
        self.executed.append(statement)
        if "__nope__" in statement:
            raise Exception("boom: table not found")
        return None


# ----------- Tests


def test_execute_maps_success_and_failure_without_leakage():
    # Given a 2-action plan and the statements it compiles to (OK → FAIL)
    plan = ActionPlan(actions=(AddColumn(Column("a", Integer())), DropColumn("b")))
    statements = ["SELECT 1", "SELECT * FROM __nope__"]

    # When we execute the statements against the plan
    summary = _execute_statements(_FakeSpark(), plan, statements)

    # Then the success and failure are mapped with correct metadata and no leakage
    results = summary.results
    assert [r.action for r in results] == ["AddColumn", "DropColumn"]
    assert [r.action_index for r in results] == [0, 1]
    assert isinstance(results[0], ExecutionSucceeded)
    assert isinstance(results[1], ExecutionFailed)
    assert results[1].failure is not None


def test_execute_stops_at_first_failure_to_avoid_half_migrating():
    # Given a 3-action plan whose middle statement fails
    spark = _FakeSpark()
    plan = ActionPlan(
        actions=(
            AddColumn(Column("a", Integer())),
            DropColumn("b"),
            AddColumn(Column("c", Integer())),
        ),
    )
    statements = [
        "SELECT 1",  # OK
        "SELECT * FROM __nope__",  # FAIL
        "SELECT 2",  # must NOT run -- the plan stops at the failure
    ]

    # When we execute the statements against the plan
    summary = _execute_statements(spark, plan, statements)

    # Then execution stops at the failure: only the first two statements run,
    # and the third (after the failure) never does
    assert spark.executed == statements[:2]

    # And the report covers only the attempted actions, ending at the failure
    results = summary.results
    assert [type(r) for r in results] == [ExecutionSucceeded, ExecutionFailed]
    assert [r.action_index for r in results] == [0, 1]


def test_execute_returns_empty_summary_for_empty_plan():
    # Given an empty plan
    plan = ActionPlan(actions=())

    # When we execute the plan
    summary = DatabricksExecutor(_FakeSpark()).execute(_dummy_qualified_name(), plan)

    # Then nothing ran and the summary is empty and non-failing
    assert summary.results == ()
    assert summary.failed is False


def test_createtable_action_creates_table_with_correct_schema(spark, temp_schema):
    # Given a desired table to be created in an empty schema
    desired = DesiredTable(
        qualified_name=QualifiedName(TEST_CATALOG, temp_schema, "customers"),
        columns=(Column(name="id", data_type=Integer()),),
    )
    plan = ActionPlan(actions=(CreateTable(table=desired),))

    # When we apply the plan (compile → execute)
    DatabricksExecutor(spark).execute(desired.qualified_name, plan)

    # Then the table exists and its schema matches exactly
    assert spark.catalog.tableExists(str(desired.qualified_name))
    actual_schema = spark.table(str(desired.qualified_name)).schema
    expected_schema = T.StructType([T.StructField("id", T.IntegerType(), nullable=True)])
    assert actual_schema == expected_schema


def test_addcolumn_action_adds_column_to_existing_table(spark, make_temp_table):
    # Given an existing Delta table with a single column
    fq = make_temp_table("add_col", "id INT NOT NULL")
    qualified_name = QualifiedName(*fq.split("."))
    plan = ActionPlan(actions=(AddColumn(column=Column(name="age", data_type=Integer())),))

    # When we apply the plan
    DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the new column exists with the expected type
    age_field = _get_field(spark, fq, "age")
    assert age_field.dataType.simpleString() == "int"


def test_dropcolumn_action_removes_column_from_existing_table(spark, make_temp_table):
    # Given an existing Delta table with column mapping enabled (required for DROP COLUMN)
    fq = make_temp_table(
        "drop_col",
        "id INT NOT NULL, to_remove STRING",
        tblprops={"delta.columnMapping.mode": "name"},
    )
    qualified_name = QualifiedName(*fq.split("."))
    plan = ActionPlan(actions=(DropColumn(column_name="to_remove"),))

    # When we apply the plan
    DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the column no longer exists
    assert "to_remove" not in spark.table(fq).columns


def test_setproperty_action_sets_table_property(spark, make_temp_table):
    # Given an existing Delta table
    fq = make_temp_table("set_prop", "id INT NOT NULL")
    qualified_name = QualifiedName(*fq.split("."))
    prop = "engine.test.setproperty"
    plan = ActionPlan(actions=(SetProperty(name=prop, value="yes"),))

    # When we apply the plan
    DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the property exists with the expected value
    assert _get_table_props(spark, fq).get(prop) == "yes"


def test_setcolumncomment_sets_comment_on_column(spark, make_temp_table):
    # Given an existing Delta table with a 'name' column
    fq = make_temp_table("col_comment", "id INT NOT NULL, name STRING")
    qualified_name = QualifiedName(*fq.split("."))
    plan = ActionPlan(actions=(SetColumnComment(column_name="name", comment="customer name"),))

    # When we apply the plan
    DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the column metadata contains the new comment
    field = _get_field(spark, fq, "name")
    assert dict(field.metadata).get("comment") == "customer name"


def test_settablecomment_sets_comment_on_table(spark, make_temp_table):
    # Given an existing Delta table
    fq = make_temp_table("tbl_comment", "id INT NOT NULL")
    qualified_name = QualifiedName(*fq.split("."))
    plan = ActionPlan(actions=(SetTableComment(comment="staging table"),))

    # When we apply the plan
    DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the comment is set on the table
    assert _get_table_comment(spark, fq) == "staging table"


def test_setcolumnnullability_sets_nullable(spark, make_temp_table):
    # Given an existing table with a NOT NULL 'id' column
    fq = make_temp_table("nullability", "id INT NOT NULL, name STRING")
    qualified_name = QualifiedName(*fq.split("."))
    plan = ActionPlan((SetColumnNullability("id", True),))

    # When we apply the plan
    DatabricksExecutor(spark).execute(qualified_name, plan)

    # Then the column becomes nullable
    assert _get_field(spark, fq, "id").nullable is True
