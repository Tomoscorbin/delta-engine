"""
Direct tests for the reader's Spark-specific row -> domain mappers.

The clustering tests exercise the shared ``clustering_columns_from_detail_row``
with real pyspark ``Row`` objects, pinning the duck-typed row contract against
the genuine Spark shape. Column mapping goes through the shared DDL type
parser, so no live SparkSession is needed anywhere here.
"""

from types import SimpleNamespace

from pyspark.sql import Row
from pyspark.sql.catalog import Column as SparkColumn
import pytest

from delta_engine.adapters.databricks.spark.reader import SparkReader, _to_column_mapping
from delta_engine.adapters.databricks.sql.rows import clustering_columns_from_detail_row
from delta_engine.application.ports import ReadFailed
from delta_engine.domain.model import QualifiedName


def test_clustering_columns_mapper_returns_empty_when_field_absent():
    # Given a DESCRIBE DETAIL row from an older Delta without the field
    row = Row(properties={})
    # Then no clustering is reported (must not break reads of non-clustered tables)
    assert clustering_columns_from_detail_row(row) == ()


def test_clustering_columns_mapper_returns_empty_for_empty_array():
    row = Row(clusteringColumns=[])
    assert clustering_columns_from_detail_row(row) == ()


def test_clustering_columns_mapper_lowercases_names():
    row = Row(clusteringColumns=["Region", "STORE"])
    assert clustering_columns_from_detail_row(row) == ("region", "store")


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


def test_unmappable_partition_column_fails_the_read() -> None:
    # A skipped partition column would fabricate PartitioningChanged drift,
    # so the read must fail loudly instead.
    column = spark_column(name="part", dataType="void", isPartition=True)

    with pytest.raises(RuntimeError, match="partition"):
        _to_column_mapping(column, QualifiedName("dev", "silver", "orders"))


def test_unmappable_non_partition_column_is_still_skipped() -> None:
    column = spark_column(name="extra", dataType="void", isPartition=False)

    assert _to_column_mapping(column, QualifiedName("dev", "silver", "orders")) is None


def test_fetch_state_returns_read_failed_for_unmappable_partition_column() -> None:
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
