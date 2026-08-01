import pyspark.sql.types as T

from delta_engine.adapters.databricks.spark._runner import SparkSqlRunner
from delta_engine.adapters.databricks.spark.executor import SparkExecutor
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedColumn,
    QualifiedName,
)
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


def _apply(spark, plan: ActionPlan) -> None:
    """Compile and execute each statement, as the engine drives the adapter."""
    executor = SparkExecutor(SparkSqlRunner(spark))
    for statement in executor.compile(plan).statements:
        executor.execute(statement)


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
        target=_dummy_qualified_name(),
        actions=(
            AddColumn(DesiredColumn("age", Integer())),
            DropColumn(ObservedColumn("legacy", Integer())),
        ),
    )

    # When compiling and executing the plan
    _apply(spark, plan)

    # Then the compiled SQL is sent to Spark in action order
    assert spark.executed == [
        "ALTER TABLE `cat`.`sch`.`tbl` ADD COLUMN `age` INT",
        "ALTER TABLE `cat`.`sch`.`tbl` DROP COLUMN `legacy`",
    ]


def test_empty_plan_executes_no_statements():
    # Given an empty plan
    plan = ActionPlan(target=_dummy_qualified_name())
    spark = _FakeSpark()

    # When compiling and executing the plan
    _apply(spark, plan)

    # Then nothing ran
    assert spark.executed == []


# ----------- Tests against real local Spark/Delta (auto-marked local_e2e via the
# spark fixture; see tests/conftest.py:pytest_collection_modifyitems)


def test_create_table_action_creates_table_with_correct_schema(spark, temp_schema):
    # Given a desired table to be created in an empty schema
    desired = DesiredTable(
        qualified_name=QualifiedName(TEST_CATALOG, temp_schema, "customers"),
        columns=(DesiredColumn(name="id", data_type=Integer()),),
    )
    plan = ActionPlan(target=desired.qualified_name, actions=(CreateTable(table=desired),))

    # When applying the plan
    _apply(spark, plan)

    # Then the table exists and its schema matches exactly
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
        target=qualified_name,
        actions=(
            AddColumn(
                column=DesiredColumn(
                    name="age",
                    data_type=Integer(),
                )
            ),
        ),
    )

    # When applying the plan
    _apply(spark, plan)

    # Then the new column exists with the expected type
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
    plan = ActionPlan(
        target=qualified_name,
        actions=(DropColumn(ObservedColumn("to_remove", Integer())),),
    )

    # When applying the plan
    _apply(spark, plan)

    # Then the column no longer exists
    assert "to_remove" not in spark.table(full_table_name).columns


def test_set_property_action_sets_table_property(spark, make_temp_table):
    # Given an existing Delta table
    full_table_name = make_temp_table("set_prop", "id INT NOT NULL")
    qualified_name = QualifiedName(*full_table_name.split("."))
    property_name = "engine.test.setproperty"
    plan = ActionPlan(
        target=qualified_name,
        actions=(
            SetProperty(
                name=property_name,
                desired_value="yes",
                observed_value=None,
            ),
        ),
    )

    # When applying the plan
    _apply(spark, plan)

    # Then the property exists with the expected value
    assert _get_table_props(spark, full_table_name).get(property_name) == "yes"


def test_set_column_comment_sets_comment_on_column(spark, make_temp_table):
    # Given an existing Delta table with a name column
    full_table_name = make_temp_table("col_comment", "id INT NOT NULL, name STRING")
    qualified_name = QualifiedName(*full_table_name.split("."))
    plan = ActionPlan(
        target=qualified_name,
        actions=(
            SetColumnComment(
                column_name="name",
                desired_comment="customer name",
                observed_comment="",
            ),
        ),
    )

    # When applying the plan
    _apply(spark, plan)

    # Then the column metadata contains the new comment
    field = _get_field(spark, full_table_name, "name")
    assert dict(field.metadata).get("comment") == "customer name"


def test_set_table_comment_sets_comment_on_table(spark, make_temp_table):
    # Given an existing Delta table
    full_table_name = make_temp_table("tbl_comment", "id INT NOT NULL")
    qualified_name = QualifiedName(*full_table_name.split("."))
    plan = ActionPlan(
        target=qualified_name,
        actions=(SetTableComment(desired_comment="staging table", observed_comment=""),),
    )

    # When applying the plan
    _apply(spark, plan)

    # Then the table comment is set
    assert _get_table_comment(spark, full_table_name) == "staging table"


def test_set_column_nullability_sets_nullable(spark, make_temp_table):
    # Given an existing table with a NOT NULL id column
    full_table_name = make_temp_table("nullability", "id INT NOT NULL, name STRING")
    qualified_name = QualifiedName(*full_table_name.split("."))
    plan = ActionPlan(
        target=qualified_name,
        actions=(
            SetColumnNullability(
                column_name="id",
                desired_nullable=True,
                observed_nullable=False,
            ),
        ),
    )

    # When applying the plan
    _apply(spark, plan)

    # Then the column becomes nullable
    assert _get_field(spark, full_table_name, "id").nullable is True


def test_compile_returns_the_statements_execute_would_run():
    # Given a plan with one action
    qualified_name = QualifiedName("cat", "schema", "tbl")
    plan = ActionPlan(
        target=qualified_name,
        actions=(SetTableComment(desired_comment="hello", observed_comment=""),),
    )
    executor = SparkExecutor(SparkSqlRunner(_FakeSpark()))

    # When compiling without executing
    compiled = executor.compile(plan)

    # Then the statements match the SQL compiler's output, in plan order
    assert compiled == compile_plan(plan)
    assert len(compiled.statements) == 1
    assert "COMMENT" in compiled.statements[0].upper()


def test_compile_of_empty_plan_returns_no_statements():
    executor = SparkExecutor(SparkSqlRunner(_FakeSpark()))
    plan = ActionPlan(target=QualifiedName("cat", "schema", "tbl"))
    assert executor.compile(plan).statements == ()
