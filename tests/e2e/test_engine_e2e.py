from uuid import uuid4

import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.executor import DatabricksExecutor
from delta_engine.adapters.databricks.reader import DatabricksReader
from delta_engine.api import Column, Date, DeltaTable, Integer, String
from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.failures import ValidationFailure
from delta_engine.application.report import TableRunStatus
from tests.config import TEST_CATALOG


def _patch_table_exists_for_local(monkeypatch):
    def _table_exists(self, qualified_name):
        # Local Spark fallback for existence checks
        return self.spark.catalog.tableExists(f"{qualified_name.schema}.{qualified_name.name}")

    # raising=True so a rename of _table_exists fails loudly here rather than
    # silently leaving every e2e test running against the unpatched method.
    monkeypatch.setattr(DatabricksReader, "_table_exists", _table_exists, raising=True)


def test_engine_sync_happy_path(spark, monkeypatch, temp_schema):
    # Given a desired table definition in an empty temp schema
    _patch_table_exists_for_local(monkeypatch)
    table_name = f"e2e_happy_{uuid4().hex[:8]}"

    engine = Engine(reader=DatabricksReader(spark), executor=DatabricksExecutor(spark))

    # When we sync
    report = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("name", String()),
            ),
            comment="E2E happy path table",
        )
    )

    # Then the run is reported as a successful SyncReport for the caller
    assert report.any_failures is False

    # And the table exists with expected schema and comment
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    df = spark.table(fq).limit(0)
    fields = {f.name: f for f in df.schema.fields}

    assert isinstance(fields["id"].dataType, T.IntegerType)
    assert fields["id"].nullable is False

    assert isinstance(fields["name"].dataType, T.StringType)
    assert fields["name"].nullable is True

    assert spark.catalog.getTable(fq).description == "E2E happy path table"


def test_engine_sync_adds_nullable_and_drops_columns_happy_path(spark, monkeypatch, temp_schema):
    # Given an existing Delta table with id, name, to_remove
    _patch_table_exists_for_local(monkeypatch)
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
    engine = Engine(reader=DatabricksReader(spark), executor=DatabricksExecutor(spark))

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


def test_engine_sync_fails_when_adding_non_nullable_column(spark, monkeypatch, temp_schema):
    # Given an existing Delta table with id, name
    _patch_table_exists_for_local(monkeypatch)
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
    engine = Engine(reader=DatabricksReader(spark), executor=DatabricksExecutor(spark))

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
                comment="unchanged",
            )
        )

    # Then the table is reported VALIDATION_FAILED with a validation failure,
    # and the error message names the offending column so an operator can act
    [table_report] = excinfo.value.report.table_reports
    assert table_report.status is TableRunStatus.VALIDATION_FAILED
    assert any(isinstance(f, ValidationFailure) for f in table_report.failures)
    assert "age" in str(excinfo.value)

    # And the schema is unchanged: the invalid 'age' column was never added
    cols = {f.name for f in spark.table(fq).schema.fields}
    assert "age" not in cols


def test_engine_idempotent_when_already_in_desired_state(spark, monkeypatch, temp_schema):
    _patch_table_exists_for_local(monkeypatch)

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
    engine = Engine(DatabricksReader(spark), DatabricksExecutor(spark))

    engine.sync(*tables)

    # When syncing a second time against an already-correct table
    second_report = engine.sync(*tables)

    # Then no actions were executed (true no-op, not just a schema-equal re-apply)
    assert all(t.execution is None for t in second_report.table_reports)
    assert spark.catalog.getTable(fq).description == "idempotency test"


def test_engine_loosen_nullability_sets_column_nullable(
    spark, monkeypatch, make_temp_table, temp_schema
):
    _patch_table_exists_for_local(monkeypatch)

    fq = make_temp_table(
        "nullable",
        "id INT NOT NULL, name STRING",
        tblprops={"delta.columnMapping.mode": "name"},
    )

    engine = Engine(DatabricksReader(spark), DatabricksExecutor(spark))
    engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            fq.split(".")[-1],
            columns=(Column("id", Integer(), nullable=True), Column("name", String())),
            comment="unchanged",
        )
    )

    field = next(f for f in spark.table(fq).schema.fields if f.name == "id")
    assert isinstance(field.dataType, T.IntegerType)
    assert field.nullable is True


def test_engine_creates_partitioned_table_with_expected_partitions(spark, monkeypatch, temp_schema):
    _patch_table_exists_for_local(monkeypatch)

    table_name = f"part_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"

    engine = Engine(DatabricksReader(spark), DatabricksExecutor(spark))
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


def test_engine_isolates_failures_and_applies_successful_tables(spark, monkeypatch, temp_schema):
    _patch_table_exists_for_local(monkeypatch)

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

    engine = Engine(DatabricksReader(spark), DatabricksExecutor(spark))
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
            ),  # invalid add
        )
    assert bad in str(excinfo.value)

    ok_fields = {f.name: f for f in spark.table(fq_ok).schema.fields}
    assert "age" in ok_fields and ok_fields["age"].nullable is True

    bad_cols = {f.name for f in spark.table(fq_bad).schema.fields}
    assert "age" not in bad_cols


def test_engine_metadata_only_updates_comments_without_touching_schema(
    spark, monkeypatch, temp_schema
):
    # Given an existing table with an extra live column and no comments
    _patch_table_exists_for_local(monkeypatch)
    table_name = f"e2e_meta_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(f"CREATE TABLE {fq} (id INT, extra_col STRING) USING DELTA")

    engine = Engine(reader=DatabricksReader(spark), executor=DatabricksExecutor(spark))

    # When syncing a metadata-only definition that omits the extra column
    report = engine.sync(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(Column("id", Integer(), comment="surrogate key"),),
            comment="metadata-only table",
            metadata_only=True,
        )
    )

    # Then the sync succeeds, comments are applied, and the schema is untouched
    assert report.any_failures is False
    fields = {f.name: f for f in spark.table(fq).schema.fields}
    assert set(fields) == {"id", "extra_col"}  # extra_col NOT dropped
    assert fields["id"].metadata.get("comment") == "surrogate key"
    assert spark.catalog.getTable(fq).description == "metadata-only table"


def test_engine_metadata_only_fails_when_a_commented_column_is_missing_live(
    spark, monkeypatch, temp_schema
):
    # Given an existing table that lacks the declared 'ghost' column
    _patch_table_exists_for_local(monkeypatch)
    table_name = f"e2e_meta_drift_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(f"CREATE TABLE {fq} (id INT) USING DELTA")

    engine = Engine(reader=DatabricksReader(spark), executor=DatabricksExecutor(spark))

    # When syncing metadata that targets the missing column
    with pytest.raises(SyncFailedError) as exc_info:
        engine.sync(
            DeltaTable(
                TEST_CATALOG,
                temp_schema,
                table_name,
                columns=(
                    Column("id", Integer()),
                    Column("ghost", String(), comment="cannot land"),
                ),
                metadata_only=True,
            )
        )

    # Then the failure is a plan-time validation failure naming the column,
    # and the live table is unchanged
    report = exc_info.value.report.table_reports[0]
    assert report.status is TableRunStatus.VALIDATION_FAILED
    assert any(
        isinstance(failure, ValidationFailure) and "ghost" in failure.message
        for failure in report.failures
    )
    assert {f.name for f in spark.table(fq).schema.fields} == {"id"}
