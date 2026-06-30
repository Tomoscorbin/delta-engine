from __future__ import annotations

from types import SimpleNamespace

from pyspark.errors.exceptions.base import AnalysisException
import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.reader import DatabricksReader
from delta_engine.application.results import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint

# ---------- fakes & helpers ----------


class FakeDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def head(self, n):
        return self._rows[:n]

    def first(self):
        return self._rows[0] if self._rows else None

    def collect(self):
        return list(self._rows)


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

    def __init__(
        self,
        *,
        catalog: FakeCatalog | None = None,
        fk_rows: list | None = None,
        fk_raises: Exception | None = None,
    ):
        self.catalog = catalog or FakeCatalog()
        self._fk_rows = fk_rows
        self._fk_raises = fk_raises

    def sql(self, query: str):
        if "referential_constraints" in query:
            if self._fk_raises is not None:
                raise self._fk_raises
            return FakeDataFrame(self._fk_rows or [])
        raise NotImplementedError(f"unexpected query: {query!r}")


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

    def sql(self, query: str):
        if "referential_constraints" in query:
            return FakeDataFrame([])
        if self._describe_exc is not None:
            raise self._describe_exc
        return FakeDataFrame(self._describe_rows or [])


class FakeSparkWithPrimaryKey:
    """
    Spark fake that handles both DESCRIBE DETAIL (properties) and the
    INFORMATION_SCHEMA primary key query.

    sql() is called for both queries; this fake distinguishes them by
    the presence of 'information_schema' in the query string.
    """

    def __init__(
        self,
        *,
        catalog: FakeCatalog,
        describe_rows=None,
        pk_column_rows=None,
        pk_exc: Exception | None = None,
    ):
        self._catalog = catalog
        self._describe_rows = describe_rows or [{"properties": {}}]
        self._pk_column_rows = pk_column_rows or []
        self._pk_exc = pk_exc

    @property
    def catalog(self):
        return self._catalog

    def sql(self, query: str):
        if "referential_constraints" in query:
            # FK query — always return empty; these tests focus on PK behaviour
            return FakeDataFrame([])
        if "information_schema" in query.lower():
            if self._pk_exc is not None:
                raise self._pk_exc
            return FakeDataFrame(self._pk_column_rows)
        return FakeDataFrame(self._describe_rows)


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
    spark = FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=[{"properties": {}}])

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
    spark = FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=[{"properties": {}}])

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
    spark = FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=[{"properties": {}}])

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
        FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=describe_rows),
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
        FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=[]),
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the table is present with empty properties and the expected comment
    assert isinstance(result, TablePresent)
    assert dict(result.table.properties) == {}
    assert result.table.comment == ""


def test_fetch_state_returns_failed_when_spark_raises_analysis_exception():
    # Given a table that exists but DESCRIBE DETAIL raises AnalysisException
    qn = QualifiedName("c", "s", "boom")
    fq = str(qn)
    catalog = FakeCatalog(columns_by_table={fq: []}, table_comments={fq: ""})
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
        FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=[{"properties": {}}])
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the table fails: skipping its only column leaves zero columns, which
    # violates the TableSnapshot invariant and surfaces as a ReadFailed
    assert isinstance(result, ReadFailed)


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
        FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=[{"properties": {}}])
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
        FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=[{"properties": {}}])
    )

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then names are normalised to lowercase at the adapter boundary (no crash)
    assert isinstance(result, TablePresent)
    assert [c.name for c in result.table.columns] == ["eventid", "username"]
    assert result.table.partitioned_by == ("username",)


# ---------- tests: primary key ----------


def test_fetch_primary_key_returns_column_names_from_information_schema():
    # Given: information_schema returns one column name for the PK
    qn = QualifiedName("c", "s", "t")
    spark = FakeSparkWithPrimaryKey(
        catalog=FakeCatalog(),
        pk_column_rows=[{"column_name": "id"}],
    )

    # When
    reader = DatabricksReader(spark)
    pk = reader._fetch_primary_key(qn)

    # Then: the primary key column names are returned
    assert pk == ("id",)


def test_fetch_primary_key_returns_empty_when_no_pk_defined():
    # Given: information_schema returns no rows (no PK)
    qn = QualifiedName("c", "s", "t")
    spark = FakeSparkWithPrimaryKey(
        catalog=FakeCatalog(),
        pk_column_rows=[],
    )

    # When
    reader = DatabricksReader(spark)
    pk = reader._fetch_primary_key(qn)

    # Then: empty tuple
    assert pk == ()


def test_fetch_primary_key_lowercases_column_names():
    # Given: information_schema returns a mixed-case column name
    qn = QualifiedName("c", "s", "t")
    spark = FakeSparkWithPrimaryKey(
        catalog=FakeCatalog(),
        pk_column_rows=[{"column_name": "OrderID"}],
    )

    # When
    reader = DatabricksReader(spark)
    pk = reader._fetch_primary_key(qn)

    # Then: name is normalised to lowercase
    assert pk == ("orderid",)


def test_fetch_state_includes_primary_key_in_observed_table():
    # Given: a table with a PK
    qn = QualifiedName("c", "s", "t")
    fq = str(qn)
    catalog = FakeCatalog(
        columns_by_table={
            fq: [make_catalog_col("id", dataType=T.IntegerType(), nullable=False)],
        },
        table_comments={fq: ""},
    )
    spark = FakeSparkWithPrimaryKey(
        catalog=catalog,
        describe_rows=[{"properties": {}}],
        pk_column_rows=[{"column_name": "id"}],
    )

    # When
    result = DatabricksReader(spark).fetch_state(qn)

    # Then: primary_key is populated on the ObservedTable
    assert isinstance(result, TablePresent)
    assert result.table.primary_key == ("id",)


def test_fetch_state_primary_key_is_empty_when_none_defined():
    # Given: a table with no PK
    qn = QualifiedName("c", "s", "t")
    fq = str(qn)
    catalog = FakeCatalog(
        columns_by_table={
            fq: [make_catalog_col("id", dataType=T.IntegerType())],
        },
        table_comments={fq: ""},
    )
    spark = FakeSparkWithPrimaryKey(
        catalog=catalog,
        describe_rows=[{"properties": {}}],
        pk_column_rows=[],
    )

    # When
    result = DatabricksReader(spark).fetch_state(qn)

    # Then: primary_key is empty
    assert isinstance(result, TablePresent)
    assert result.table.primary_key == ()


def test_fetch_primary_key_returns_empty_when_information_schema_unavailable():
    # Given: information_schema raises AnalysisException (non-Unity Catalog Spark)
    qn = QualifiedName("c", "s", "t")
    fq = str(qn)
    catalog = FakeCatalog(
        columns_by_table={fq: [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={fq: ""},
    )
    spark = FakeSparkWithPrimaryKey(
        catalog=catalog,
        describe_rows=[{"properties": {}}],
        pk_exc=AnalysisException("TABLE_OR_VIEW_NOT_FOUND"),
    )

    # When
    result = DatabricksReader(spark).fetch_state(qn)

    # Then: the read succeeds and primary_key is empty (no UC = no PK constraints)
    assert isinstance(result, TablePresent)
    assert result.table.primary_key == ()


# ---------- tests: foreign keys ----------


def test_fetch_foreign_keys_returns_single_column_fk():
    # Given rows from information_schema describing one FK
    rows = [
        SimpleNamespace(
            constraint_name="orders_customer_id_fk",
            local_column="customer_id",
            ordinal_position=1,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="id",
        )
    ]
    spark = FakeSpark(fk_rows=rows)
    reader = DatabricksReader(spark)

    # When
    fks = reader._fetch_foreign_keys(QualifiedName("cat", "sch", "orders"))

    # Then
    assert len(fks) == 1
    assert fks[0] == ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
        constraint_name="orders_customer_id_fk",
    )


def test_fetch_foreign_keys_returns_composite_fk_in_ordinal_order():
    # Given rows for a composite FK (ordinal order: tenant_id first, then customer_id)
    rows = [
        SimpleNamespace(
            constraint_name="orders_comp_fk",
            local_column="tenant_id",
            ordinal_position=1,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="tenant_id",
        ),
        SimpleNamespace(
            constraint_name="orders_comp_fk",
            local_column="customer_id",
            ordinal_position=2,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="id",
        ),
    ]
    spark = FakeSpark(fk_rows=rows)
    reader = DatabricksReader(spark)

    # When
    fks = reader._fetch_foreign_keys(QualifiedName("cat", "sch", "orders"))

    # Then columns are preserved in ordinal order
    assert len(fks) == 1
    assert fks[0].local_columns == ("tenant_id", "customer_id")
    assert fks[0].referenced_columns == ("tenant_id", "id")


def test_fetch_foreign_keys_returns_empty_when_no_fks():
    # Given no FK rows for this table
    spark = FakeSpark(fk_rows={})
    reader = DatabricksReader(spark)

    # When
    fks = reader._fetch_foreign_keys(QualifiedName("cat", "sch", "orders"))

    # Then
    assert fks == ()


def test_fetch_foreign_keys_returns_empty_on_analysis_exception():
    # Given Spark raises AnalysisException (non-UC environment without information_schema)
    spark = FakeSpark(fk_raises=AnalysisException("no information_schema"))
    reader = DatabricksReader(spark)

    # When
    fks = reader._fetch_foreign_keys(QualifiedName("cat", "sch", "orders"))

    # Then falls back gracefully
    assert fks == ()
