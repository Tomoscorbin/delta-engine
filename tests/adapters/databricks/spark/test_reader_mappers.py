"""
Direct tests for the reader's Spark-specific row -> domain mappers.

No fakes: ``_clustering_columns_from_row`` takes a plain DESCRIBE DETAIL row
and returns a domain value directly.

The column-mapping tests below are the exception: ``_to_column_mapping`` parses
each Spark DDL type string through ``SparkType.fromDDL``, which needs a live
SparkSession, so those tests request the ``spark`` fixture directly.
"""

from types import SimpleNamespace

from pyspark.sql import Row
from pyspark.sql.catalog import Column as SparkColumn
import pytest

from delta_engine.adapters.databricks.spark.reader import (
    SparkReader,
    _clustering_columns_from_row,
    _to_column_mapping,
)
from delta_engine.application.ports import ReadFailed
from delta_engine.domain.model import QualifiedName


def test_clustering_columns_mapper_returns_empty_when_field_absent():
    # Given a DESCRIBE DETAIL row from an older Delta without the field
    row = Row(properties={})
    # Then no clustering is reported (must not break reads of non-clustered tables)
    assert _clustering_columns_from_row(row) == ()


def test_clustering_columns_mapper_returns_empty_for_empty_array():
    row = Row(clusteringColumns=[])
    assert _clustering_columns_from_row(row) == ()


def test_clustering_columns_mapper_lowercases_names():
    row = Row(clusteringColumns=["Region", "STORE"])
    assert _clustering_columns_from_row(row) == ("region", "store")


# ---------- column mapping ----------


def spark_column(
    *,
    name: str,
    dataType: str,
    isPartition: bool,
    description: str | None = None,
    nullable: bool = True,
    isBucket: bool = False,
) -> SparkColumn:
    """Build a real pyspark catalog ``Column``, matching what Unity Catalog returns."""
    return SparkColumn(
        name=name,
        description=description,
        dataType=dataType,
        nullable=nullable,
        isPartition=isPartition,
        isBucket=isBucket,
        isCluster=False,
    )


def test_unmappable_partition_column_fails_the_read(spark) -> None:
    # A skipped partition column would fabricate PartitioningChanged drift,
    # so the read must fail loudly instead.
    column = spark_column(name="part", dataType="void", isPartition=True)

    with pytest.raises(RuntimeError, match="partition"):
        _to_column_mapping(column, QualifiedName("dev", "silver", "orders"))


def test_unmappable_non_partition_column_is_still_skipped(spark) -> None:
    column = spark_column(name="extra", dataType="void", isPartition=False)

    assert _to_column_mapping(column, QualifiedName("dev", "silver", "orders")) is None


def test_fetch_state_returns_read_failed_for_unmappable_partition_column(spark) -> None:
    # Given a catalog whose table has a partition column the engine cannot map
    bad_column = spark_column(name="part", dataType="void", isPartition=True)
    stub_spark = SimpleNamespace(
        catalog=SimpleNamespace(
            tableExists=lambda name: True,
            listColumns=lambda name: [bad_column],
        )
    )

    # When fetching state through the port
    state = SparkReader(stub_spark).fetch_state(QualifiedName("dev", "silver", "orders"))

    # Then the mapper's refusal surfaces as a typed ReadFailed, not an exception
    assert isinstance(state, ReadFailed)
    assert "partition" in state.failure.message.lower()
