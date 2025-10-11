from uuid import uuid4

import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.catalog.executor import DatabricksExecutor
from delta_engine.adapters.databricks.catalog.reader import DatabricksReader
from delta_engine.adapters.schema import Column, Date, DeltaTable, Integer, String
from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.registry import Registry
from tests.config import TEST_CATALOG, TEST_SCHEMA


def _patch_table_exists_for_local(monkeypatch):
    def _table_exists(self, qualified_name):
        # Local Spark fallback for existence checks
        return self.spark.catalog.tableExists(f"{qualified_name.schema}.{qualified_name.name}")

    monkeypatch.setattr(DatabricksReader, "_table_exists", _table_exists, raising=False)


@pytest.fixture
def temp_schema(spark):
    """
    Create a unique schema (database) for the test and drop it afterwards.
    Keeps the workspace clean even if the test fails (CASCADE).
    """
    schema = f"{TEST_SCHEMA}_tmp_{uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {schema}")
    try:
        yield schema
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture
def make_temp_table(spark, temp_schema):
    created = []

    def _create(name_prefix: str, columns_sql: str, *, tblprops: dict[str, str] | None = None):
        name = f"{name_prefix}_{uuid4().hex[:8]}"
        fq = f"{TEST_CATALOG}.{temp_schema}.{name}"
        props = ""
        if tblprops:
            # e.g. {"delta.columnMapping.mode":"name"} -> "('k'='v',...)"
            items = ", ".join(f"'{k}'='{v}'" for k, v in tblprops.items())
            props = f"TBLPROPERTIES ({items})"
        spark.sql(f"CREATE TABLE {fq} ({columns_sql}) USING DELTA {props}")
        created.append(fq)
        return fq

    try:
        yield _create
    finally:
        for fq in created:
            spark.sql(f"DROP TABLE IF EXISTS {fq}")


def test_engine_sync_happy_path(spark, monkeypatch, temp_schema):
    # Given a desired table definition in an empty temp schema
    _patch_table_exists_for_local(monkeypatch)
    table_name = f"e2e_happy_{uuid4().hex[:8]}"
    registry = Registry()
    registry.register(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), is_nullable=False),
                Column("name", String()),
            ),
            comment="E2E happy path table",
        )
    )

    engine = Engine(reader=DatabricksReader(spark), executor=DatabricksExecutor(spark))

    # When we sync
    engine.sync(registry)

    # Then table exists with expected schema and comment
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
    registry = Registry()
    registry.register(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), is_nullable=False),
                Column("name", String()),
                Column("age", Integer(), is_nullable=True),  # new column, nullable (valid)
            ),
            comment="unchanged",
        )
    )

    engine = Engine(reader=DatabricksReader(spark), executor=DatabricksExecutor(spark))

    # When we sync desired -> observed
    engine.sync(registry)

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
    registry = Registry()
    registry.register(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), is_nullable=False),
                Column("name", String()),
                Column("age", Integer(), is_nullable=False),  # non-nullable add -> invalid
            ),
            comment="unchanged",
        )
    )

    engine = Engine(reader=DatabricksReader(spark), executor=DatabricksExecutor(spark))

    # When we sync desired -> observed
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(registry)

    # Then validation failed for non-nullable add and schema unchanged (no 'age' column)
    msg = str(excinfo.value)
    assert "VALIDATION_FAILED" in msg
    assert "NonNullableColumnAdd" in msg
    assert "age" in msg

    cols = {f.name for f in spark.table(fq).schema.fields}
    assert "age" not in cols


def test_engine_idempotent_when_already_in_desired_state(spark, monkeypatch, temp_schema):
    _patch_table_exists_for_local(monkeypatch)

    table_name = f"idem_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"

    reg = Registry()
    reg.register(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(Column("id", Integer(), is_nullable=False), Column("name", String())),
            comment="idempotency test",
        )
    )
    engine = Engine(DatabricksReader(spark), DatabricksExecutor(spark))

    engine.sync(reg)
    first = spark.table(fq).schema.jsonValue()

    engine.sync(reg)
    second = spark.table(fq).schema.jsonValue()

    assert first == second
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

    reg = Registry()
    reg.register(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            fq.split(".")[-1],
            columns=(Column("id", Integer(), is_nullable=True), Column("name", String())),
            comment="unchanged",
        )
    )
    engine = Engine(DatabricksReader(spark), DatabricksExecutor(spark))
    engine.sync(reg)

    field = next(f for f in spark.table(fq).schema.fields if f.name == "id")
    assert isinstance(field.dataType, T.IntegerType)
    assert field.nullable is True


def test_engine_creates_partitioned_table_with_expected_partitions(spark, monkeypatch, temp_schema):
    _patch_table_exists_for_local(monkeypatch)

    table_name = f"part_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"

    reg = Registry()
    reg.register(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            table_name,
            columns=(
                Column("id", Integer(), is_nullable=False),
                Column("p_date", Date()),
                Column("store", String()),
            ),
            partitioned_by=("p_date", "store"),
            comment="partitioned table",
        )
    )
    engine = Engine(DatabricksReader(spark), DatabricksExecutor(spark))
    engine.sync(reg)

    assert spark.catalog.tableExists(f"{temp_schema}.{table_name}")
    parts = tuple(c.name for c in spark.catalog.listColumns(fq) if getattr(c, "isPartition", False))
    assert parts == ("p_date", "store")
    assert spark.catalog.getTable(fq).description == "partitioned table"


def test_engine_isolates_failures_and_applies_successful_tables(
    spark, monkeypatch, temp_schema, make_temp_table
):
    _patch_table_exists_for_local(monkeypatch)

    ok = f"ok_{uuid4().hex[:8]}"
    bad = f"bad_{uuid4().hex[:8]}"
    fq_ok = f"{TEST_CATALOG}.{temp_schema}.{ok}"
    fq_bad = f"{TEST_CATALOG}.{temp_schema}.{bad}"

    # seed both tables
    for _ in (fq_ok, fq_bad):
        make_temp_table(
            "seed_ignored",
            "id INT NOT NULL, name STRING",
            tblprops={"delta.columnMapping.mode": "name"},
        )  # created and auto-dropped; we need specific names instead:
    spark.sql(
        f"CREATE TABLE {fq_ok} (id INT NOT NULL, name STRING)"
        " USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name')"
    )
    spark.sql(
        f"CREATE TABLE {fq_bad} (id INT NOT NULL, name STRING)"
        " USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name')"
    )

    reg = Registry()
    reg.register(
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            ok,
            columns=(
                Column("id", Integer(), is_nullable=False),
                Column("name", String()),
                Column("age", Integer(), is_nullable=True),
            ),
        ),
        DeltaTable(
            TEST_CATALOG,
            temp_schema,
            bad,
            columns=(
                Column("id", Integer(), is_nullable=False),
                Column("name", String()),
                Column("age", Integer(), is_nullable=False),
            ),
        ),  # invalid add
    )

    engine = Engine(DatabricksReader(spark), DatabricksExecutor(spark))
    with pytest.raises(SyncFailedError) as excinfo:
        engine.sync(reg)
    assert bad in str(excinfo.value)

    ok_fields = {f.name: f for f in spark.table(fq_ok).schema.fields}
    assert "age" in ok_fields and ok_fields["age"].nullable is True

    bad_cols = {f.name for f in spark.table(fq_bad).schema.fields}
    assert "age" not in bad_cols
