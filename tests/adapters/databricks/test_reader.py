from __future__ import annotations

from types import SimpleNamespace

import pyspark.sql.types as T
from pyspark.sql.utils import AnalysisException
import pytest

from delta_engine.adapters.databricks.reader import DatabricksReader
from delta_engine.application.results import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName

# ---------- fakes & helpers ----------


class FakeDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def head(self, n):
        return self._rows[:n]

    def first(self):
        return self._rows[0] if self._rows else None


class FakeCatalog:
    def __init__(
        self,
        *,
        columns_by_table=None,
        table_comments=None,
        exists: bool = True,
        exists_exc: Exception | None = None,
    ):
        self._columns_by_table = columns_by_table or {}
        self._table_comments = table_comments or {}
        self._exists = exists
        self._exists_exc = exists_exc

    def tableExists(self, fully_qualified_name: str) -> bool:
        if self._exists_exc is not None:
            raise self._exists_exc
        return self._exists

    def listColumns(self, fully_qualified_name: str):
        return self._columns_by_table.get(fully_qualified_name, [])

    def getTable(self, fully_qualified_name: str):
        # Only `description` is read by the code under test
        return SimpleNamespace(description=self._table_comments.get(fully_qualified_name, ""))


class FakeSpark:
    """Spark fake whose catalog.* calls delegate to the provided FakeCatalog."""

    def __init__(self, *, catalog: FakeCatalog | None = None):
        self.catalog = catalog or FakeCatalog()


class FakeSparkProps:
    """
    Spark fake for _fetch_properties() unit tests.

    - sql(query) returns a dataframe built from `rows`
    """

    def __init__(self, rows):
        self._rows = rows
        self.catalog = SimpleNamespace()  # not used in these tests

    def sql(self, _query: str):
        return FakeDataFrame(self._rows)


class FakeSparkForFetchState:
    """
    Spark fake for fetch_state().

      catalog.tableExists() -> existence probe (configured on the FakeCatalog)
      sql() -> DESCRIBE DETAIL (returns rows or raises)
    """

    def __init__(
        self,
        *,
        catalog: FakeCatalog,
        describe_rows=None,
        describe_exc: Exception | None = None,
    ):
        self._catalog = catalog
        self._describe_rows = describe_rows
        self._describe_exc = describe_exc

    @property
    def catalog(self):
        return self._catalog

    def sql(self, _query: str):
        if self._describe_exc is not None:
            raise self._describe_exc
        return FakeDataFrame(self._describe_rows or [])


def make_catalog_col(
    name: str,
    *,
    dataType="string",
    nullable: bool = True,
    description: str = "",
    isPartition: bool = False,
):
    """Build a duck-typed SparkColumn for the reader."""
    return SimpleNamespace(
        name=name,
        dataType=dataType,
        nullable=nullable,
        description=description,
        isPartition=isPartition,
    )


# ---------- shared fixtures ----------


@pytest.fixture
def qn() -> QualifiedName:
    return QualifiedName("c", "s", "t")


# ---------- tests: columns & partitions ----------


def test_columns_maps_name_nullability_and_comment(qn):
    # Given a catalog exposing two columns
    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                make_catalog_col(
                    "id", dataType=T.IntegerType(), nullable=False, description="identifier"
                ),
                make_catalog_col("p_date", dataType=T.DateType(), nullable=True, description=""),
            ]
        }
    )
    spark = FakeSparkForFetchState(catalog=catalog, describe_rows=[{"properties": {}}])

    # When we fetch state for the table
    result = DatabricksReader(spark).fetch_state(qn)

    # Then names, nullability, and comments are mapped correctly
    assert isinstance(result, TablePresent)
    cols = result.table.columns
    assert [c.name for c in cols] == ["id", "p_date"]
    assert [c.nullable for c in cols] == [False, True]
    assert [c.comment for c in cols] == ["identifier", ""]


def test_partition_columns_returns_only_partition_names_in_order(qn):
    # Given a mix of regular and partition columns
    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                make_catalog_col("id", dataType=T.IntegerType(), isPartition=False),
                make_catalog_col("p_store", dataType=T.StringType(), isPartition=True),
                make_catalog_col("p_date", dataType=T.DateType(), isPartition=True),
            ]
        }
    )
    spark = FakeSparkForFetchState(catalog=catalog, describe_rows=[{"properties": {}}])

    # When we fetch state for the table
    result = DatabricksReader(spark).fetch_state(qn)

    # Then only partition columns are returned and ordering is preserved
    assert isinstance(result, TablePresent)
    assert result.table.partitioned_by == ("p_store", "p_date")


def test_partition_columns_ignores_missing_or_false_flags():
    # Given columns with isPartition absent or False
    class NoIsPartition(SimpleNamespace):
        pass

    qn = QualifiedName("c", "s", "u")
    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                NoIsPartition(name="a", dataType=T.IntegerType(), nullable=True, description=""),
                make_catalog_col("b", dataType=T.StringType(), isPartition=False),
            ]
        }
    )
    spark = FakeSparkForFetchState(catalog=catalog, describe_rows=[{"properties": {}}])

    # When we fetch state for the table
    result = DatabricksReader(spark).fetch_state(qn)

    # Then no partitions are reported
    assert isinstance(result, TablePresent)
    assert result.table.partitioned_by == ()


# ---------- tests: properties ----------


def test_fetch_properties_returns_empty_readonly_when_describe_has_no_rows(qn):
    # Given DESCRIBE DETAIL yields no rows
    reader = DatabricksReader(FakeSparkProps(rows=[]))

    # When we fetch properties
    props = reader._fetch_properties(qn)

    # Then we get an empty mapping
    assert dict(props) == {}
    with pytest.raises(TypeError):
        props["x"] = "y"  # type: ignore[index]


def test_fetch_properties_returns_full_catalog_map_unfiltered(qn):
    # Given DESCRIBE DETAIL returns properties including ones the engine does not manage
    rows = [
        {
            "properties": {
                "delta.columnMapping.mode": "name",
                "delta.minReaderVersion": "2",
                "custom.unlisted": "kept",
            }
        }
    ]
    reader = DatabricksReader(FakeSparkProps(rows=rows))

    # When we fetch properties
    props = reader._fetch_properties(qn)

    # Then the full catalog map passes through unfiltered, as a read-only mapping
    assert dict(props) == {
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "custom.unlisted": "kept",
    }
    with pytest.raises(TypeError):
        props["x"] = "y"


# ---------- tests: table comment ----------


@pytest.mark.parametrize(
    "desc_value, expected", [("orders table", "orders table"), (None, ""), ("", "")]
)
def test_fetch_table_comment_returns_description_or_empty(desc_value, expected):
    # Given a catalog that may or may not have a description
    qualified_name = QualifiedName("c", "s", "t")
    catalog = FakeCatalog(table_comments={str(qualified_name): desc_value})
    reader = DatabricksReader(FakeSpark(catalog=catalog))

    # When we fetch the table comment
    comment = reader._fetch_table_comment(qualified_name)

    # Then we get the description or an empty string
    assert comment == expected


def test_fetch_state_returns_absent_when_table_does_not_exist(qn):
    # Given a reader whose existence probe returns empty
    reader = DatabricksReader(FakeSparkForFetchState(catalog=FakeCatalog(exists=False)))

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the catalog reports the table as absent
    assert isinstance(result, TableAbsent)


def test_fetch_state_returns_present_with_columns_partitions_comment_and_properties():
    # Given a table that exists with two columns (one partition), a comment, and properties
    qn = QualifiedName("c", "s", "t")
    fq = str(qn)

    catalog = FakeCatalog(
        columns_by_table={
            fq: [
                make_catalog_col(
                    "id", dataType=T.IntegerType(), nullable=False, description="identifier"
                ),
                make_catalog_col("p_date", dataType=T.DateType(), isPartition=True),
            ]
        },
        table_comments={fq: "orders table"},
    )
    describe_rows = [
        {
            "properties": {
                "delta.columnMapping.mode": "name",
                "delta.deletedFileRetentionDuration": "interval 1 day",
            }
        }
    ]

    reader = DatabricksReader(
        FakeSparkForFetchState(catalog=catalog, describe_rows=describe_rows),
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the observed payload contains correct columns, partitions, comment, and properties
    assert isinstance(result, TablePresent)
    observed = result.table
    assert [c.name for c in observed.columns] == ["id", "p_date"]
    assert observed.partitioned_by == ("p_date",)
    assert observed.comment == "orders table"
    assert dict(observed.properties) == {
        "delta.columnMapping.mode": "name",
        "delta.deletedFileRetentionDuration": "interval 1 day",
    }


def test_fetch_state_returns_present_with_empty_properties_when_describe_has_no_rows():
    # Given a table that exists but DESCRIBE DETAIL returns no rows
    qn = QualifiedName("c", "s", "no_props")
    fq = str(qn)

    catalog = FakeCatalog(
        columns_by_table={fq: [make_catalog_col("id", dataType=T.IntegerType(), nullable=False)]},
        table_comments={fq: ""},
    )
    reader = DatabricksReader(
        FakeSparkForFetchState(catalog=catalog, describe_rows=[]),
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the table is present with empty properties and the expected comment
    assert isinstance(result, TablePresent)
    assert dict(result.table.properties) == {}
    assert result.table.comment == ""


def test_fetch_state_returns_failed_when_spark_raises_analysis_exception():
    # Given a table that exists with a mappable column but DESCRIBE DETAIL raises
    qn = QualifiedName("c", "s", "boom")
    fq = str(qn)
    catalog = FakeCatalog(
        columns_by_table={fq: [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={fq: ""},
    )
    reader = DatabricksReader(
        FakeSparkForFetchState(catalog=catalog, describe_exc=AnalysisException("kaboom"))
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the result encodes failure with the Spark exception type
    assert isinstance(result, ReadFailed)
    assert result.failure.exception_type == "AnalysisException"


def test_fetch_state_returns_failed_when_existence_probe_raises():
    # Given the existence probe itself raises (e.g. the namespace is missing)
    qn = QualifiedName("c", "s", "missing_ns")
    reader = DatabricksReader(
        FakeSparkForFetchState(
            catalog=FakeCatalog(exists_exc=AnalysisException("namespace not found")),
        )
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the failure is isolated to this table rather than escaping the reader
    assert isinstance(result, ReadFailed)
    assert result.failure.exception_type == "AnalysisException"


def test_fetch_state_returns_failed_when_all_columns_are_unsupported():
    # Given a table whose only column has a type the engine cannot map (BinaryType)
    qn = QualifiedName("c", "s", "structy")
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("payload", dataType=T.BinaryType())]},
        table_comments={str(qn): ""},
    )
    reader = DatabricksReader(
        FakeSparkForFetchState(catalog=catalog, describe_rows=[{"properties": {}}])
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the table fails: with every column skipped, the reader reports that
    # none of its types are mappable rather than degrading to an empty table
    assert isinstance(result, ReadFailed)
    assert "unmappable Spark types" in result.failure.message


def test_fetch_state_skips_unsupported_column_leaves_mappable_columns_intact():
    # Given a table with one unsupported column (BinaryType) and two supported ones
    qn = QualifiedName("c", "s", "mixed")
    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                make_catalog_col("id", dataType=T.IntegerType()),
                make_catalog_col("payload", dataType=T.BinaryType()),
                make_catalog_col("name", dataType=T.StringType()),
            ]
        },
        table_comments={str(qn): ""},
    )
    reader = DatabricksReader(
        FakeSparkForFetchState(catalog=catalog, describe_rows=[{"properties": {}}])
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the table is present with only the two mappable columns; payload is dropped
    assert isinstance(result, TablePresent)
    assert [c.name for c in result.table.columns] == ["id", "name"]


def test_fetch_state_lowercases_mixed_case_column_names_from_catalog():
    # Given a catalog (e.g. Hive Metastore) that preserves mixed-case column names
    qn = QualifiedName("c", "s", "hms")
    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                make_catalog_col("EventId", dataType=T.IntegerType()),
                make_catalog_col("UserName", dataType=T.StringType(), isPartition=True),
            ]
        },
        table_comments={str(qn): ""},
    )
    reader = DatabricksReader(
        FakeSparkForFetchState(catalog=catalog, describe_rows=[{"properties": {}}])
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then names are normalised to lowercase at the adapter boundary (no crash)
    assert isinstance(result, TablePresent)
    assert [c.name for c in result.table.columns] == ["eventid", "username"]
    assert result.table.partitioned_by == ("username",)
