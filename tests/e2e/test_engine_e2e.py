import json
from uuid import uuid4

import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.spark._runner import SparkSqlRunner
from delta_engine.adapters.databricks.spark.executor import SparkExecutor
from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.failures import ValidationFailure
from delta_engine.application.report import TableRunStatus
from delta_engine.schema import Column, Date, Decimal, DeltaTable, Double, Integer, Long, String
from tests.adapters.databricks.native_reader import NativeSparkReader
from tests.config import TEST_CATALOG

pytestmark = pytest.mark.local_e2e


def _spark_executor(spark):
    return SparkExecutor(SparkSqlRunner(spark))


def test_engine_sync_happy_path(spark, temp_schema):
    # Given a desired table definition in an empty temp schema
    table_name = f"e2e_happy_{uuid4().hex[:8]}"

    engine = Engine(reader=NativeSparkReader(spark), executor=_spark_executor(spark))

    # When we sync
    report = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("name", String(), comment="customer name"),
            ),
            comment="E2E happy path table",
        )
    )

    # Then the run is reported as a successful SyncReport for the caller
    assert report.has_failures is False

    # And the table exists with expected schema and comment
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    df = spark.table(fq).limit(0)
    fields = {f.name: f for f in df.schema.fields}

    assert isinstance(fields["id"].dataType, T.IntegerType)
    assert fields["id"].nullable is False

    assert isinstance(fields["name"].dataType, T.StringType)
    assert fields["name"].nullable is True
    assert fields["name"].metadata.get("comment") == "customer name"

    assert spark.catalog.getTable(fq).description == "E2E happy path table"


def test_engine_preserves_spark_variable_expressions_in_comments(spark, temp_schema):
    variable_substitution = "spark.sql.variable.substitute"
    original_setting = spark.conf.get(variable_substitution)
    table_name = f"e2e_literal_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    literal = "documentation containing ${env:HOME}"
    declaration = DeltaTable(
        TEST_CATALOG,
        temp_schema,
        table_name,
        columns=(Column("id", Integer(), comment=literal),),
        comment=literal,
    )
    engine = Engine(reader=NativeSparkReader(spark), executor=_spark_executor(spark))

    # Spark normally substitutes ${...} before parsing, even inside quoted SQL
    # literals. Exercise that default explicitly so this remains a regression
    # test for the runner's session guard rather than merely for SQL quoting.
    spark.conf.set(variable_substitution, "true")
    try:
        report = engine.sync(declaration)

        assert report.has_failures is False
        assert spark.catalog.getTable(fq).description == literal
        [field] = spark.table(fq).schema.fields
        assert field.metadata.get("comment") == literal
        assert spark.conf.get(variable_substitution) == "true"

        second_report = engine.sync(declaration)

        assert len(second_report.table_runs[0].plan) == 0
        assert spark.conf.get(variable_substitution) == "true"
    finally:
        spark.conf.set(variable_substitution, original_setting)


def test_engine_sync_adds_nullable_and_drops_columns_happy_path(spark, temp_schema):
    # Given an existing Delta table with id, name, to_remove
    table_name = f"e2e_cols_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(
        f"""
        CREATE TABLE {fq}
        (id INT NOT NULL, name STRING, to_remove STRING)
        USING DELTA
        TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
        """
    )

    # And a desired definition that drops `to_remove` and adds `age` (nullable)
    engine = Engine(reader=NativeSparkReader(spark), executor=_spark_executor(spark))

    # When we sync desired -> observed
    engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("name", String()),
                Column("age", Integer(), nullable=True),  # new column, nullable (valid)
            ),
            comment="unchanged",
            properties={"delta.columnMapping.mode": "name"},
        )
    )

    # Then the table schema reflects the add/drop with correct nullability
    df = spark.table(fq).limit(0)
    fields_by_name = {f.name: f for f in df.schema.fields}

    # Dropped column is gone
    assert "to_remove" not in fields_by_name

    # Existing columns preserved
    assert isinstance(fields_by_name["id"].dataType, T.IntegerType)
    assert fields_by_name["id"].nullable is False

    assert isinstance(fields_by_name["name"].dataType, T.StringType)
    assert fields_by_name["name"].nullable is True

    # Added column present and nullable
    assert isinstance(fields_by_name["age"].dataType, T.IntegerType)
    assert fields_by_name["age"].nullable is True

    # Column order (id, name, age) - append new at end is acceptable
    assert [f.name for f in df.schema.fields] == ["id", "name", "age"]


def test_engine_sync_fails_when_adding_non_nullable_column(spark, temp_schema):
    # Given an existing Delta table with id, name
    table_name = f"e2e_vfail_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(
        f"""
        CREATE TABLE {fq}
        (id INT NOT NULL, name STRING)
        USING DELTA
        TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
        """
    )

    # And a desired definition that adds a NOT NULL column 'age' (should fail validation)
    engine = Engine(reader=NativeSparkReader(spark), executor=_spark_executor(spark))

    # When we sync desired -> observed
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(
            DeltaTable(
                TEST_CATALOG,
                temp_schema,
                table_name,
                columns=(
                    Column("id", Integer(), nullable=False),
                    Column("name", String()),
                    Column("age", Integer(), nullable=False),  # non-nullable add -> invalid
                ),
                properties={"delta.columnMapping.mode": "name"},
                comment="unchanged",
            )
        )

    # Then the table is reported PLANNING_FAILED with a validation failure,
    # and the error message names the offending column so an operator can act
    [table_report] = excinfo.value.report.table_runs
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert any(isinstance(f, ValidationFailure) for f in table_report.failures)
    assert "age" in str(excinfo.value)

    # And the schema is unchanged: the invalid 'age' column was never added
    cols = {f.name for f in spark.table(fq).schema.fields}
    assert "age" not in cols


def test_engine_idempotent_when_already_in_desired_state(spark, temp_schema):
    table_name = f"idem_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"

    tables = [
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(Column("id", Integer(), nullable=False), Column("name", String())),
            comment="idempotency test",
        )
    ]
    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))

    engine.sync(*tables)

    # When syncing a second time against an already-correct table
    second_report = engine.sync(*tables)

    # Then no actions were executed (true no-op, not just a schema-equal re-apply)
    assert all(t.execution is None for t in second_report.table_runs)
    assert spark.catalog.getTable(fq).description == "idempotency test"


def test_engine_sync_applies_evolving_declaration_over_multiple_runs(spark, temp_schema):
    table_name = f"lifecycle_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))

    initial = DeltaTable(
        TEST_CATALOG,
        temp_schema,
        table_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("name", String()),
            Column("legacy", String()),
        ),
        comment="customer table v1",
        properties={"delta.columnMapping.mode": "name"},
    )
    first_report = engine.sync(initial)
    assert first_report.has_failures is False

    evolved = DeltaTable(
        TEST_CATALOG,
        temp_schema,
        table_name,
        columns=(
            Column("id", Integer(), nullable=False, comment="stable id"),
            Column("name", String(), comment="display name"),
            Column("age", Integer(), comment="age in years"),
        ),
        comment="customer table v2",
        properties={
            "delta.columnMapping.mode": "name",
            "delta.logRetentionDuration": "interval 30 days",
        },
    )
    second_report = engine.sync(evolved)
    assert second_report.has_failures is False
    assert second_report.table_runs[0].execution is not None

    fields = {f.name: f for f in spark.table(fq).schema.fields}
    assert set(fields) == {"id", "name", "age"}
    assert isinstance(fields["id"].dataType, T.IntegerType)
    assert fields["id"].nullable is False
    assert fields["id"].metadata.get("comment") == "stable id"
    assert isinstance(fields["name"].dataType, T.StringType)
    assert fields["name"].metadata.get("comment") == "display name"
    assert isinstance(fields["age"].dataType, T.IntegerType)
    assert fields["age"].nullable is True
    assert fields["age"].metadata.get("comment") == "age in years"
    assert spark.catalog.getTable(fq).description == "customer table v2"

    detail = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0]
    assert detail["properties"].get("delta.columnMapping.mode") == "name"
    assert detail["properties"].get("delta.logRetentionDuration") == "interval 30 days"

    third_report = engine.sync(evolved)
    assert len(third_report.table_runs[0].plan) == 0


def test_engine_loosen_nullability_sets_column_nullable(spark, make_temp_table, temp_schema):
    fq = make_temp_table(
        "nullable",
        "id INT NOT NULL, name STRING",
        tblprops={"delta.columnMapping.mode": "name"},
    )

    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))
    engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            fq.split(".")[-1],
            columns=(Column("id", Integer(), nullable=True), Column("name", String())),
            comment="unchanged",
            properties={"delta.columnMapping.mode": "name"},
        )
    )

    field = next(f for f in spark.table(fq).schema.fields if f.name == "id")
    assert isinstance(field.dataType, T.IntegerType)
    assert field.nullable is True


def test_engine_creates_partitioned_table_with_expected_partitions(spark, temp_schema):
    table_name = f"part_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"

    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))
    engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("p_date", Date()),
                Column("store", String()),
            ),
            partitioned_by=("p_date", "store"),
            comment="partitioned table",
        )
    )

    assert spark.catalog.tableExists(f"{temp_schema}.{table_name}")
    parts = tuple(c.name for c in spark.catalog.listColumns(fq) if getattr(c, "isPartition", False))
    assert parts == ("p_date", "store")
    assert spark.catalog.getTable(fq).description == "partitioned table"


def test_engine_isolates_failures_and_applies_successful_tables(spark, temp_schema):
    ok = f"ok_{uuid4().hex[:8]}"
    bad = f"bad_{uuid4().hex[:8]}"
    fq_ok = f"{TEST_CATALOG}.{temp_schema}.{ok}"
    fq_bad = f"{TEST_CATALOG}.{temp_schema}.{bad}"

    # seed both tables with specific names the sync will target
    spark.sql(
        f"CREATE TABLE {fq_ok} (id INT NOT NULL, name STRING)"
        " USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name')"
    )
    spark.sql(
        f"CREATE TABLE {fq_bad} (id INT NOT NULL, name STRING)"
        " USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name')"
    )

    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(
            DeltaTable(
                TEST_CATALOG,
                temp_schema,
                ok,
                columns=(
                    Column("id", Integer(), nullable=False),
                    Column("name", String()),
                    Column("age", Integer(), nullable=True),
                ),
                properties={"delta.columnMapping.mode": "name"},
            ),
            DeltaTable(
                TEST_CATALOG,
                temp_schema,
                bad,
                columns=(
                    Column("id", Integer(), nullable=False),
                    Column("name", String()),
                    Column("age", Integer(), nullable=False),
                ),
                properties={"delta.columnMapping.mode": "name"},
            ),  # invalid add
        )
    assert bad in str(excinfo.value)

    ok_fields = {f.name: f for f in spark.table(fq_ok).schema.fields}
    assert "age" in ok_fields and ok_fields["age"].nullable is True

    bad_cols = {f.name for f in spark.table(fq_bad).schema.fields}
    assert "age" not in bad_cols


def test_engine_metadata_scope_applies_comments_when_schema_matches(spark, temp_schema):
    # Given an existing table whose schema exactly matches the declaration
    table_name = f"e2e_meta_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(f"CREATE TABLE {fq} (id INT) USING DELTA")

    engine = Engine(reader=NativeSparkReader(spark), executor=_spark_executor(spark))

    # When syncing a metadata-only definition
    report = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(Column("id", Integer(), comment="surrogate key"),),
            comment="metadata-only table",
            scope="metadata",
        )
    )

    # Then the sync succeeds, comments are applied, and the schema is untouched
    assert report.has_failures is False
    fields = {f.name: f for f in spark.table(fq).schema.fields}
    assert set(fields) == {"id"}
    assert fields["id"].metadata.get("comment") == "surrogate key"
    details = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0]
    assert details["description"] == "metadata-only table"


def test_engine_metadata_scope_fails_when_column_type_has_drifted(spark, temp_schema):
    # Given an existing table whose column type differs from the declaration
    table_name = f"e2e_meta_drift_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(f"CREATE TABLE {fq} (id BIGINT) USING DELTA")  # BIGINT, declared as INT

    engine = Engine(reader=NativeSparkReader(spark), executor=_spark_executor(spark))

    # When syncing a metadata-only definition that declares id as Integer
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            DeltaTable(
                TEST_CATALOG,
                temp_schema,
                table_name,
                columns=(Column("id", Integer()),),
                scope="metadata",
            )
        )

    # Then validation failed before execution and the table is unchanged
    report = exc_info.value.report.table_runs[0]
    assert report.status is TableRunStatus.PLANNING_FAILED
    assert {f.name for f in spark.table(fq).schema.fields} == {"id"}


def test_engine_unsets_property_declared_absent(spark, temp_schema):
    # Given a table carrying a property the declaration asserts absent
    table_name = f"prop_unset_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(
        f"CREATE TABLE {fq} (id INT NOT NULL) USING DELTA"
        " TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"
    )

    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))

    # When syncing a declaration stating the key must be absent
    engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(Column("id", Integer(), nullable=False),),
            properties={"delta.enableChangeDataFeed": None},
        )
    )

    # Then the key is removed from the catalog
    detail = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0]
    assert "delta.enableChangeDataFeed" not in detail["properties"]


def test_engine_fails_loud_on_undeclared_column_mapping(spark, temp_schema):
    # Given a table with column mapping enabled but a declaration that omits it
    table_name = f"prop_loud_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(
        f"CREATE TABLE {fq} (id INT NOT NULL) USING DELTA"
        " TBLPROPERTIES ('delta.columnMapping.mode'='name')"
    )

    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))

    # When / Then the sync fails at validation, naming the property
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(
            DeltaTable(
                TEST_CATALOG,
                temp_schema,
                table_name,
                columns=(Column("id", Integer(), nullable=False),),
            )
        )
    table_report = excinfo.value.report.table_runs[0]
    assert any("delta.columnMapping.mode" in f.message for f in table_report.failures)


def test_engine_ignores_platform_written_properties(spark, temp_schema):
    # Given a table carrying unmanaged platform-style keys — the reader
    # filters them out of the observed state before the domain sees them
    table_name = f"prop_invis_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(
        f"CREATE TABLE {fq} (id INT NOT NULL) USING DELTA"
        " TBLPROPERTIES ('delta.appendOnly'='false')"
    )

    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))

    # When syncing a declaration that says nothing about properties
    report = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(Column("id", Integer(), nullable=False),),
        )
    )

    # Then the unmanaged key is invisible — no failure, no action
    assert report.has_failures is False
    assert len(report.table_runs[0].plan) == 0


def test_engine_property_sync_is_idempotent(spark, temp_schema):
    # Given a declaration synced once — also detects Delta value normalization:
    # if the catalog stores a normalized form of a declared value, the second
    # sync would plan a spurious SetProperty and this test fails
    table_name = f"prop_idem_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    declaration = DeltaTable(
        TEST_CATALOG,
        temp_schema,
        table_name,
        columns=(Column("id", Integer(), nullable=False),),
        properties={
            "delta.enableChangeDataFeed": "true",
            "delta.logRetentionDuration": "interval 30 days",
        },
    )
    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))
    engine.sync(declaration)
    detail = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0]
    assert detail["properties"].get("delta.enableChangeDataFeed") == "true"
    assert detail["properties"].get("delta.logRetentionDuration") == "interval 30 days"

    # When syncing the identical declaration again
    report = engine.sync(declaration)

    # Then nothing is planned
    assert len(report.table_runs[0].plan) == 0


def test_engine_creates_and_reclusters_a_table(spark, temp_schema):
    # Given a clustered table declaration
    table_name = f"cluster_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))

    def _clustering_columns() -> list[str]:
        detail = spark.sql(f"DESCRIBE DETAIL {fq}").collect()[0]
        return list(detail["clusteringColumns"])

    # When creating the table clustered by `region`
    first = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("region", String()),
            ),
            clustered_by=("region",),
            comment="clustered table",
        )
    )

    # Then the catalog reports `region` as the clustering column
    assert first.has_failures is False
    assert _clustering_columns() == ["region"]

    # When the clustering key changes to `id`
    reclustered = DeltaTable(
        TEST_CATALOG,
        temp_schema,
        table_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("region", String()),
        ),
        clustered_by=("id",),
        comment="clustered table",
    )
    second = engine.sync(reclustered)

    # Then it is reconciled in place (an action ran) and the catalog reflects it
    assert second.has_failures is False
    assert second.table_runs[0].execution is not None
    assert _clustering_columns() == ["id"]

    # And an identical re-sync is a true no-op (idempotent; order-insensitive set diff)
    third = engine.sync(reclustered)
    assert len(third.table_runs[0].plan) == 0


def test_engine_sync_widens_column_types_with_type_widening_declared(spark, temp_schema):
    # Given an existing Delta table with columns the declaration will widen
    table_name = f"e2e_widen_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(f"CREATE TABLE {fq} (id INT, amount DECIMAL(10,2), ratio INT) USING DELTA")

    engine = Engine(reader=NativeSparkReader(spark), executor=_spark_executor(spark))

    # When we sync a declaration widening each column, enabling widening in the same sync
    # (amount grows scale with precision; ratio crosses to Double)
    report = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Long()),
                Column("amount", Decimal(12, 3)),
                Column("ratio", Double()),
            ),
            properties={"delta.enableTypeWidening": "true"},
        )
    )

    # Then the sync succeeds and every column is widened in place
    assert report.has_failures is False
    fields = {f.name: f for f in spark.table(fq).schema.fields}
    assert isinstance(fields["id"].dataType, T.LongType)
    assert fields["amount"].dataType == T.DecimalType(12, 3)
    assert isinstance(fields["ratio"].dataType, T.DoubleType)


def test_dry_run_report_is_ci_consumable(spark, temp_schema):
    # Given a declared table that does not exist yet (dry run plans a CREATE)
    table_name = f"e2e_dry_ci_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"

    engine = Engine(reader=NativeSparkReader(spark), executor=_spark_executor(spark))

    # When we sync as a dry run
    report = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("name", String()),
            ),
        ),
        dry_run=True,
    )

    # Then the gate booleans, SQL, and projection are all consistent
    assert report.has_changes is True
    assert report.has_failures is False

    [table_report] = list(report)
    assert table_report.compiled is not None
    assert table_report.compiled.statements
    statement = table_report.compiled.statements[0]
    assert "CREATE TABLE" in statement.upper()

    payload = report.to_dict()
    json.dumps(payload)  # plain types only — must not raise
    assert payload["dry_run"] is True
    assert payload["tables"][0]["planned_sql_statements"] == list(
        table_report.compiled.statements
    )

    # And nothing was created
    assert spark.catalog.tableExists(fq) is False


def test_engine_renames_column_preserving_data_and_is_idempotent(spark, temp_schema):
    # Given a column-mapped table with data under the old column name
    table_name = f"rename_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(
        f"CREATE TABLE {fq} (id INT NOT NULL, customer_nm STRING) USING DELTA"
        " TBLPROPERTIES ('delta.columnMapping.mode'='name')"
    )
    spark.sql(f"INSERT INTO {fq} VALUES (1, 'Ada')")

    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))
    declaration = DeltaTable(
        TEST_CATALOG,
        temp_schema,
        table_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("customer_name", String(), renamed_from="customer_nm"),
        ),
        properties={"delta.columnMapping.mode": "name"},
    )

    # When we sync, the column is renamed in place and its data is preserved
    report = engine.sync(declaration)
    assert report.has_failures is False

    rows = spark.sql(f"SELECT id, customer_name FROM {fq}").collect()
    assert [(r.id, r.customer_name) for r in rows] == [(1, "Ada")]

    # And a second sync sees no drift — the hint is inert once applied
    second = engine.sync(declaration)
    assert second.has_failures is False
    assert second.has_changes is False


def test_engine_renames_partition_and_clustering_columns_without_layout_churn(spark, temp_schema):
    # Given mapped tables whose old column names are layout keys
    partition_table = f"rename_partition_{uuid4().hex[:8]}"
    cluster_table = f"rename_cluster_{uuid4().hex[:8]}"
    partition_fq = f"{TEST_CATALOG}.{temp_schema}.{partition_table}"
    cluster_fq = f"{TEST_CATALOG}.{temp_schema}.{cluster_table}"
    properties = {"delta.columnMapping.mode": "name"}
    engine = Engine(NativeSparkReader(spark), _spark_executor(spark))

    created = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            partition_table,
            columns=(Column("id", Integer()), Column("event_day", String())),
            properties=properties,
            partitioned_by=("event_day",),
        ),
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            cluster_table,
            columns=(Column("id", Integer()), Column("customer_nm", String())),
            properties=properties,
            clustered_by=("customer_nm",),
        ),
    )
    assert created.has_failures is False

    # When both layout columns are renamed
    renamed_partition = DeltaTable(
        TEST_CATALOG,
        temp_schema,
        partition_table,
        columns=(
            Column("id", Integer()),
            Column("event_date", String(), renamed_from="event_day"),
        ),
        properties=properties,
        partitioned_by=("event_date",),
    )
    renamed_cluster = DeltaTable(
        TEST_CATALOG,
        temp_schema,
        cluster_table,
        columns=(
            Column("id", Integer()),
            Column("customer_name", String(), renamed_from="customer_nm"),
        ),
        properties=properties,
        clustered_by=("customer_name",),
    )
    report = engine.sync(renamed_partition, renamed_cluster)

    # Then each plan contains only its rename; Delta carries layout identity
    # through the mapped column's stable physical name.
    assert report.has_failures is False
    assert all(
        len(statements) == 1 and "RENAME COLUMN" in statements[0]
        for statements in report.planned_sql_statements.values()
    )
    partition_detail = spark.sql(f"DESCRIBE DETAIL {partition_fq}").collect()[0]
    cluster_detail = spark.sql(f"DESCRIBE DETAIL {cluster_fq}").collect()[0]
    assert list(partition_detail["partitionColumns"]) == ["event_date"]
    assert list(cluster_detail["clusteringColumns"]) == ["customer_name"]

    # And the projected layout names make the next sync converge.
    converged = engine.sync(renamed_partition, renamed_cluster)
    assert converged.has_failures is False
    assert converged.has_changes is False
