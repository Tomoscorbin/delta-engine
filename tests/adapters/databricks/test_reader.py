from __future__ import annotations

from types import SimpleNamespace

from pyspark.errors.exceptions.base import AnalysisException
import pyspark.sql.types as T
import pytest

from delta_engine.adapters.databricks.reader import DatabricksReader
from delta_engine.application.results import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint

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
        if "table_tags" in query.lower():
            return FakeDataFrame([])
        if self._describe_exc is not None:
            raise self._describe_exc
        return FakeDataFrame(self._describe_rows or [])


class FakeSparkWithPrimaryKey:
    """
    Spark fake that handles the queries `fetch_state` issues against a present
    table: DESCRIBE DETAIL (properties), the information_schema primary key
    query, the information_schema foreign key query, and the
    information_schema.table_tags query.

    sql() distinguishes them by query text: the FK query references
    'referential_constraints'; the tag query references 'table_tags'; the PK
    query is the other 'information_schema' query; everything else is treated as
    DESCRIBE DETAIL. The 'table_tags' check precedes the generic
    'information_schema' check because the tag query also contains that string.
    """

    def __init__(
        self,
        *,
        catalog: FakeCatalog,
        describe_rows=None,
        pk_column_rows=None,
        pk_exc: Exception | None = None,
        fk_rows=None,
        fk_exc: Exception | None = None,
        tag_rows=None,
        tags_exc: Exception | None = None,
    ):
        self._catalog = catalog
        self._describe_rows = describe_rows or [{"properties": {}}]
        self._pk_column_rows = pk_column_rows or []
        self._pk_exc = pk_exc
        self._fk_rows = fk_rows or []
        self._fk_exc = fk_exc
        self._tag_rows = tag_rows or []
        self._tags_exc = tags_exc

    @property
    def catalog(self):
        return self._catalog

    def sql(self, query: str):
        if "referential_constraints" in query:
            if self._fk_exc is not None:
                raise self._fk_exc
            return FakeDataFrame(self._fk_rows)
        if "table_tags" in query.lower():
            if self._tags_exc is not None:
                raise self._tags_exc
            return FakeDataFrame(self._tag_rows)
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


def test_observed_properties_are_empty_and_read_only_when_describe_has_no_rows(qn):
    # Given a present table whose DESCRIBE DETAIL yields no rows
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={str(qn): ""},
    )
    reader = DatabricksReader(FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=[]))

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the observed table carries an empty, read-only property mapping
    assert isinstance(result, TablePresent)
    properties = result.table.properties
    assert dict(properties) == {}
    with pytest.raises(TypeError):
        properties["x"] = "y"  # type: ignore[index]


def test_observed_properties_pass_through_catalog_map_unfiltered(qn):
    # Given a present table whose DESCRIBE DETAIL returns properties the engine does not manage
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={str(qn): ""},
    )
    describe_rows = [
        {
            "properties": {
                "delta.columnMapping.mode": "name",
                "delta.minReaderVersion": "2",
                "custom.unlisted": "kept",
            }
        }
    ]
    reader = DatabricksReader(FakeSparkWithPrimaryKey(catalog=catalog, describe_rows=describe_rows))

    # When we fetch state
    result = reader.fetch_state(qn)

    # Then the full catalog map passes through unfiltered, as a read-only mapping
    assert isinstance(result, TablePresent)
    properties = result.table.properties
    assert dict(properties) == {
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "custom.unlisted": "kept",
    }
    with pytest.raises(TypeError):
        properties["x"] = "y"  # type: ignore[index]


# ---------- tests: table comment ----------


@pytest.mark.parametrize(
    "desc_value, expected", [("orders table", "orders table"), (None, ""), ("", "")]
)
def test_observed_comment_is_description_or_empty_string(desc_value, expected):
    # Given a present table whose catalog description may be set, None, or empty
    qualified_name = QualifiedName("c", "s", "t")
    catalog = FakeCatalog(
        columns_by_table={str(qualified_name): [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={str(qualified_name): desc_value},
    )
    reader = DatabricksReader(FakeSparkWithPrimaryKey(catalog=catalog))

    # When we fetch state
    result = reader.fetch_state(qualified_name)

    # Then the observed comment is the description, or an empty string when unset
    assert isinstance(result, TablePresent)
    assert result.table.comment == expected


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


def test_fetch_state_lowercases_primary_key_column_names_from_catalog():
    # Given a present table whose information_schema reports a mixed-case PK column
    qn = QualifiedName("c", "s", "t")
    fq = str(qn)
    catalog = FakeCatalog(
        columns_by_table={fq: [make_catalog_col("orderid", dataType=T.IntegerType())]},
        table_comments={fq: ""},
    )
    spark = FakeSparkWithPrimaryKey(
        catalog=catalog,
        describe_rows=[{"properties": {}}],
        pk_column_rows=[{"column_name": "OrderID"}],
    )

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the primary key column is normalised to lowercase at the adapter boundary
    assert isinstance(result, TablePresent)
    assert result.table.primary_key == PrimaryKeyConstraint(columns=("orderid",))


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
    assert result.table.primary_key == PrimaryKeyConstraint(columns=("id",))


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

    # Then: primary_key is None (no constraint defined)
    assert isinstance(result, TablePresent)
    assert result.table.primary_key is None


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

    # Then: the read succeeds and primary_key is None (no UC = no PK constraints)
    assert isinstance(result, TablePresent)
    assert result.table.primary_key is None


# ---------- tests: foreign keys ----------


def _orders_catalog() -> FakeCatalog:
    """Build a present 'cat.sch.orders' table with the columns the FK tests reference."""
    fq = "cat.sch.orders"
    return FakeCatalog(
        columns_by_table={
            fq: [
                make_catalog_col("tenant_id", dataType=T.IntegerType()),
                make_catalog_col("customer_id", dataType=T.IntegerType()),
            ]
        },
        table_comments={fq: ""},
    )


def test_fetch_state_includes_single_column_foreign_key_in_observed_table():
    # Given a present table whose information_schema describes one FK
    qn = QualifiedName("cat", "sch", "orders")
    fk_rows = [
        SimpleNamespace(
            constraint_name="orders_customer_id_fk",
            local_column="customer_id",
            ordinal_position=1,
            position_in_unique_constraint=1,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="id",
        )
    ]
    spark = FakeSparkWithPrimaryKey(catalog=_orders_catalog(), fk_rows=fk_rows)

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the observed table carries the foreign key, named from the catalog
    assert isinstance(result, TablePresent)
    assert result.table.foreign_keys == (
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            references="cat.sch.customers",
            referenced_columns=("id",),
            constraint_name="orders_customer_id_fk",
        ),
    )


def test_fetch_state_preserves_composite_foreign_key_columns_in_ordinal_order():
    # Given a composite FK whose rows arrive in ordinal order (tenant_id, then customer_id)
    qn = QualifiedName("cat", "sch", "orders")
    fk_rows = [
        SimpleNamespace(
            constraint_name="orders_comp_fk",
            local_column="tenant_id",
            ordinal_position=1,
            position_in_unique_constraint=1,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="tenant_id",
        ),
        SimpleNamespace(
            constraint_name="orders_comp_fk",
            local_column="customer_id",
            ordinal_position=2,
            position_in_unique_constraint=2,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="id",
        ),
    ]
    spark = FakeSparkWithPrimaryKey(catalog=_orders_catalog(), fk_rows=fk_rows)

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the FK's columns are preserved in ordinal order
    assert isinstance(result, TablePresent)
    [foreign_key] = result.table.foreign_keys
    assert foreign_key.local_columns == ("tenant_id", "customer_id")
    assert foreign_key.referenced_columns == ("tenant_id", "id")


def test_fetch_state_foreign_keys_are_empty_when_none_defined():
    # Given a present table whose information_schema returns no FK rows
    qn = QualifiedName("cat", "sch", "orders")
    spark = FakeSparkWithPrimaryKey(catalog=_orders_catalog(), fk_rows=[])

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then no foreign keys are observed
    assert isinstance(result, TablePresent)
    assert result.table.foreign_keys == ()


def test_fetch_state_foreign_keys_are_empty_when_information_schema_unavailable():
    # Given the FK information_schema query raises (non-UC environment)
    qn = QualifiedName("cat", "sch", "orders")
    spark = FakeSparkWithPrimaryKey(
        catalog=_orders_catalog(),
        fk_exc=AnalysisException("no information_schema"),
    )

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the read still succeeds with no foreign keys (no UC = no FK constraints)
    assert isinstance(result, TablePresent)
    assert result.table.foreign_keys == ()


class _FakeSparkRawForeignKeyRows:
    """
    Spark fake that returns the *raw* per-column rows the production FK query
    produces, so tests exercise the reader's grouping/alignment rather than a
    pre-joined result. Each row mirrors one row of the real
    information_schema query result set.
    """

    def __init__(self, *, catalog, fk_rows, describe_rows=None):
        self._catalog = catalog
        self._fk_rows = fk_rows
        self._describe_rows = describe_rows or [{"properties": {}}]

    @property
    def catalog(self):
        return self._catalog

    def sql(self, query: str):
        if "referential_constraints" in query:
            return FakeDataFrame(self._fk_rows)
        if "information_schema" in query.lower():
            return FakeDataFrame([])  # no primary key rows for these tables
        return FakeDataFrame(self._describe_rows)


class _FakeSparkCapturingForeignKeyQuery:
    """
    Spark fake that records the SQL text emitted for the FK query so tests can
    assert on its structure, while still returning empty rows so fetch_state
    completes without error.
    """

    def __init__(self, *, catalog):
        self._catalog = catalog
        self.captured_fk_query: str | None = None

    @property
    def catalog(self):
        return self._catalog

    def sql(self, query: str):
        if "referential_constraints" in query:
            self.captured_fk_query = query
            return FakeDataFrame([])
        if "information_schema" in query.lower():
            return FakeDataFrame([])
        return FakeDataFrame([{"properties": {}}])


def test_foreign_key_query_correlates_referenced_columns_by_parent_key_position():
    # Given a spark fake that records the FK SQL query text
    qn = QualifiedName("cat", "sch", "orders")
    spark = _FakeSparkCapturingForeignKeyQuery(catalog=_orders_catalog())

    # When we fetch state (triggering the FK query)
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the captured query aligns referenced columns via position_in_unique_constraint
    # (the fix) and does NOT source referenced columns from constraint_column_usage
    # (the old, broken cross-join approach)
    assert isinstance(result, TablePresent)
    assert spark.captured_fk_query is not None, "FK query was never issued"
    fk_query = spark.captured_fk_query
    assert "position_in_unique_constraint" in fk_query, (
        "FK query must align referenced columns via position_in_unique_constraint"
    )
    assert "constraint_column_usage" not in fk_query, (
        "FK query must not source referenced columns from constraint_column_usage "
        "(that cross-join cannot align composite keys)"
    )


def test_fetch_state_composite_foreign_key_aligns_local_and_referenced_columns():
    # Given a composite FK (tenant_id, customer_id) -> customers(tenant_id, id):
    # the raw query returns one row per local column, each carrying the parent
    # key column at the matching position_in_unique_constraint.
    qn = QualifiedName("cat", "sch", "orders")
    fk_rows = [
        SimpleNamespace(
            constraint_name="orders_comp_fk",
            local_column="tenant_id",
            ordinal_position=1,
            position_in_unique_constraint=1,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="tenant_id",
        ),
        SimpleNamespace(
            constraint_name="orders_comp_fk",
            local_column="customer_id",
            ordinal_position=2,
            position_in_unique_constraint=2,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="id",
        ),
    ]
    spark = _FakeSparkRawForeignKeyRows(catalog=_orders_catalog(), fk_rows=fk_rows)

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then exactly one FK is observed, with columns positionally aligned and no duplicates
    assert isinstance(result, TablePresent)
    [foreign_key] = result.table.foreign_keys
    assert foreign_key.local_columns == ("tenant_id", "customer_id")
    assert foreign_key.referenced_columns == ("tenant_id", "id")
    assert foreign_key.references == "cat.sch.customers"


def test_fetch_state_composite_foreign_key_does_not_duplicate_columns():
    # Guard the grouping layer: N raw query rows must produce exactly N local columns
    # and N referenced columns — not NxM as a cross-join would.
    #
    # Given a 2-column composite FK whose raw query returns exactly 2 rows
    # (one per local column, each carrying its matching parent-key column):
    qn = QualifiedName("cat", "sch", "orders")
    fk_rows = [
        SimpleNamespace(
            constraint_name="orders_comp_fk",
            local_column="tenant_id",
            ordinal_position=1,
            position_in_unique_constraint=1,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="tenant_id",
        ),
        SimpleNamespace(
            constraint_name="orders_comp_fk",
            local_column="customer_id",
            ordinal_position=2,
            position_in_unique_constraint=2,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="id",
        ),
    ]
    spark = _FakeSparkRawForeignKeyRows(catalog=_orders_catalog(), fk_rows=fk_rows)

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the grouped FK has exactly 2 local columns and 2 referenced columns —
    # no duplication — and they are positionally aligned
    assert isinstance(result, TablePresent)
    [foreign_key] = result.table.foreign_keys
    assert len(foreign_key.local_columns) == 2
    assert len(foreign_key.referenced_columns) == 2
    assert foreign_key.local_columns == ("tenant_id", "customer_id")
    assert foreign_key.referenced_columns == ("tenant_id", "id")


def test_fetch_state_lowercases_foreign_key_constraint_name():
    # Given a catalog that returns a mixed-case constraint name
    qn = QualifiedName("cat", "sch", "orders")
    fk_rows = [
        SimpleNamespace(
            constraint_name="Orders_Customer_FK",
            local_column="customer_id",
            ordinal_position=1,
            position_in_unique_constraint=1,
            ref_catalog="cat",
            ref_schema="sch",
            ref_table="customers",
            ref_column="id",
        )
    ]
    spark = _FakeSparkRawForeignKeyRows(catalog=_orders_catalog(), fk_rows=fk_rows)

    # When
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the observed constraint name is normalised to lowercase
    [foreign_key] = result.table.foreign_keys
    assert foreign_key.constraint_name == "orders_customer_fk"


# ---------- tests: tags ----------


def test_fetch_state_observes_table_tags(qn):
    # Given a present table whose information_schema.table_tags carries two tags
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={str(qn): ""},
    )
    tag_rows = [
        SimpleNamespace(tag_name="env", tag_value="prod"),
        SimpleNamespace(tag_name="domain", tag_value="sales"),
    ]
    spark = FakeSparkWithPrimaryKey(catalog=catalog, tag_rows=tag_rows)

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the observed table carries the tags as a read-only mapping
    assert isinstance(result, TablePresent)
    assert dict(result.table.tags) == {"env": "prod", "domain": "sales"}
    with pytest.raises(TypeError):
        result.table.tags["x"] = "y"  # type: ignore[index]


def test_fetch_state_preserves_tag_key_case(qn):
    # Given a catalog tag with a mixed-case key (UC tag keys are case-sensitive)
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={str(qn): ""},
    )
    spark = FakeSparkWithPrimaryKey(
        catalog=catalog,
        tag_rows=[SimpleNamespace(tag_name="CostCentre", tag_value="data-eng")],
    )

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the tag key is NOT casefolded (contrast column names)
    assert isinstance(result, TablePresent)
    assert dict(result.table.tags) == {"CostCentre": "data-eng"}


def test_fetch_state_tags_are_empty_when_none_defined(qn):
    # Given a present table whose table_tags query returns no rows
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={str(qn): ""},
    )
    spark = FakeSparkWithPrimaryKey(catalog=catalog, tag_rows=[])

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then no tags are observed
    assert isinstance(result, TablePresent)
    assert dict(result.table.tags) == {}


def test_fetch_state_tags_are_empty_when_information_schema_unavailable(qn):
    # Given the table_tags query raises (non-Unity-Catalog Spark, e.g. local tests)
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("id", dataType=T.IntegerType())]},
        table_comments={str(qn): ""},
    )
    spark = FakeSparkWithPrimaryKey(
        catalog=catalog,
        tags_exc=AnalysisException("TABLE_OR_VIEW_NOT_FOUND"),
    )

    # When we fetch state
    result = DatabricksReader(spark).fetch_state(qn)

    # Then the read still succeeds with no tags (no UC = no tags to observe)
    assert isinstance(result, TablePresent)
    assert dict(result.table.tags) == {}
