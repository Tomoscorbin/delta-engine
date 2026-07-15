"""Tests for the native OSS-Spark reader that drives the local engine e2e suite."""

from uuid import uuid4

import pytest

from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.domain.model import Long, QualifiedName, String
from tests.adapters.databricks.native_reader import NativeSparkReader
from tests.config import TEST_CATALOG

pytestmark = pytest.mark.local_e2e


def test_reads_columns_with_types_nullability_and_comments(spark, make_temp_table, temp_schema):
    # Given a Delta table with a not-null commented column and a nullable one
    fq = make_temp_table("native_cols", "id BIGINT NOT NULL COMMENT 'surrogate key', region STRING")
    name = fq.split(".")[-1]

    # When the native reader reads it
    state = NativeSparkReader(spark).fetch_state(QualifiedName(TEST_CATALOG, temp_schema, name))

    # Then the columns, types, nullability, and comment are observed correctly
    assert isinstance(state, TablePresent)
    assert [c.name for c in state.table.columns] == ["id", "region"]
    assert state.table.columns[0].data_type == Long()
    assert state.table.columns[0].nullable is False
    assert state.table.columns[0].comment == "surrogate key"
    assert state.table.columns[1].data_type == String()
    assert state.table.columns[1].nullable is True


def test_reads_comment_properties_and_has_no_constraints_or_tags(spark, temp_schema):
    # Given a table with a comment, a managed property, and a platform-only property
    table_name = f"native_props_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(
        f"CREATE TABLE {fq} (id INT) USING DELTA COMMENT 'demo table'"
        " TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'delta.appendOnly'='false')"
    )

    # When the native reader reads it
    state = NativeSparkReader(spark).fetch_state(
        QualifiedName(TEST_CATALOG, temp_schema, table_name)
    )

    # Then the comment and only the registered property are observed
    assert isinstance(state, TablePresent)
    assert state.table.comment == "demo table"
    assert dict(state.table.properties) == {"delta.enableChangeDataFeed": "true"}

    # And primary/foreign keys and tags are always empty — OSS Spark has no
    # constraint metadata or information_schema to read them from
    assert state.table.primary_key is None
    assert state.table.foreign_keys == ()
    assert state.table.referencing_foreign_keys == ()
    assert dict(state.table.tags) == {}


def test_reads_partitioned_table_layout(spark, temp_schema):
    table_name = f"native_part_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(f"CREATE TABLE {fq} (id INT, region STRING) USING DELTA PARTITIONED BY (region)")

    state = NativeSparkReader(spark).fetch_state(
        QualifiedName(TEST_CATALOG, temp_schema, table_name)
    )

    assert isinstance(state, TablePresent)
    assert state.table.partitioned_by == ("region",)
    assert state.table.clustered_by == ()


def test_reads_clustered_table_layout(spark, temp_schema):
    table_name = f"native_cluster_{uuid4().hex[:8]}"
    fq = f"{TEST_CATALOG}.{temp_schema}.{table_name}"
    spark.sql(f"CREATE TABLE {fq} (id INT, region STRING) USING DELTA CLUSTER BY (region)")

    state = NativeSparkReader(spark).fetch_state(
        QualifiedName(TEST_CATALOG, temp_schema, table_name)
    )

    assert isinstance(state, TablePresent)
    assert state.table.clustered_by == ("region",)
    assert state.table.partitioned_by == ()


def test_absent_table_is_reported_absent(spark, temp_schema):
    state = NativeSparkReader(spark).fetch_state(
        QualifiedName(TEST_CATALOG, temp_schema, "does_not_exist")
    )

    assert isinstance(state, TableAbsent)
