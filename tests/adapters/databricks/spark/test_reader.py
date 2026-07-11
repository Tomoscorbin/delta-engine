"""
Shell tests for SparkReader: sequencing, normalization at the boundary,
and fetch_state's totality.

Row->domain mapping is covered directly in test_reader_mappers.py; query text
is pinned in sql/test_queries.py. The fake here routes spark.sql() by EXACT
query text (keyed by the same builders the reader uses), so no fake ever
parses SQL.
"""

from __future__ import annotations

from types import SimpleNamespace

from pyspark.errors.exceptions.base import AnalysisException
import pytest

from delta_engine.adapters.databricks.spark.reader import SparkReader
from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_detail_query,
    foreign_keys_query,
    information_schema_probe_query,
    primary_key_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.application.ports import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import ForeignKeyReference, QualifiedName
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint

# ---------- fakes & helpers ----------


class FakeDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def first(self):
        return self._rows[0] if self._rows else None

    def collect(self):
        return list(self._rows)


class FakeDetailRow(dict):
    """
    Duck-typed DESCRIBE DETAIL row.

    A real Spark ``Row`` supports attribute access (``row.properties``) and
    ``row.asDict()``; a plain dict supports neither. The shared detail-row
    mappers read ``properties`` by attribute and ``clusteringColumns`` via
    ``asDict().get(...)`` to tolerate the field's absence on older Delta
    versions, so detail-row fakes need both.
    """

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def asDict(self):
        return dict(self)


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


class RoutedSpark:
    """
    SparkSession fake whose sql() answers from an exact query-text table.

    A response value may be a list of rows (returned in a FakeDataFrame) or an
    Exception instance (raised). Unexpected query text fails the test loudly.
    """

    def __init__(self, *, catalog: FakeCatalog, responses: dict):
        self._catalog = catalog
        self._responses = responses
        self.queries: list[str] = []

    @property
    def catalog(self):
        return self._catalog

    def sql(self, query: str):
        self.queries.append(query)
        if query not in self._responses:
            raise AssertionError(f"unexpected query: {query}")
        value = self._responses[query]
        if isinstance(value, Exception):
            raise value
        return FakeDataFrame(value)


def routed_spark(
    qn: QualifiedName,
    *,
    catalog: FakeCatalog,
    describe=None,
    pk=(),
    fks=(),
    referencing_fks=(),
    tags=(),
    column_tags=(),
    probe=(),
):
    """Build a RoutedSpark with a full default response set for one table."""
    responses = {
        describe_detail_query(qn): (
            describe if describe is not None else [FakeDetailRow(properties={})]
        ),
        primary_key_query(qn): list(pk) if not isinstance(pk, Exception) else pk,
        foreign_keys_query(qn): list(fks) if not isinstance(fks, Exception) else fks,
        referencing_foreign_keys_query(qn): (
            list(referencing_fks) if not isinstance(referencing_fks, Exception) else referencing_fks
        ),
        table_tags_query(qn): list(tags) if not isinstance(tags, Exception) else tags,
        column_tags_query(qn): (
            list(column_tags) if not isinstance(column_tags, Exception) else column_tags
        ),
        information_schema_probe_query(qn.catalog): (
            list(probe) if not isinstance(probe, Exception) else probe
        ),
    }
    return RoutedSpark(catalog=catalog, responses=responses)


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


def single_column_catalog(qn: QualifiedName, **catalog_kwargs) -> FakeCatalog:
    return FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("id", dataType="int")]},
        **catalog_kwargs,
    )


def fk_row(
    *,
    constraint_name="fk_orders_customers",
    local_column="customer_id",
    ordinal_position=1,
    position_in_unique_constraint=1,
    ref_catalog="c",
    ref_schema="s",
    ref_table="customers",
    ref_column="id",
):
    return SimpleNamespace(
        constraint_name=constraint_name,
        local_column=local_column,
        ordinal_position=ordinal_position,
        position_in_unique_constraint=position_in_unique_constraint,
        ref_catalog=ref_catalog,
        ref_schema=ref_schema,
        ref_table=ref_table,
        ref_column=ref_column,
    )


def referencing_fk_row(
    *,
    constraint_name="orders_customer_fk",
    referencing_catalog="c",
    referencing_schema="s",
    referencing_table="orders",
):
    return SimpleNamespace(
        constraint_name=constraint_name,
        referencing_catalog=referencing_catalog,
        referencing_schema=referencing_schema,
        referencing_table=referencing_table,
    )


# ---------- shared fixtures ----------


@pytest.fixture
def qn() -> QualifiedName:
    return QualifiedName("c", "s", "t")


# ---------- columns & partitions ----------


def test_columns_maps_name_nullability_and_comment(qn):
    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                make_catalog_col("id", dataType="int", nullable=False, description="identifier"),
                make_catalog_col("p_date", dataType="date", nullable=True, description=""),
            ]
        }
    )
    result = SparkReader(routed_spark(qn, catalog=catalog)).fetch_state(qn)

    assert isinstance(result, TablePresent)
    cols = result.table.columns
    assert [c.name for c in cols] == ["id", "p_date"]
    assert [c.nullable for c in cols] == [False, True]
    assert [c.comment for c in cols] == ["identifier", ""]


def test_partition_columns_returns_only_partition_names_in_order(qn):
    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                make_catalog_col("id", dataType="int", isPartition=False),
                make_catalog_col("p_store", dataType="string", isPartition=True),
                make_catalog_col("p_date", dataType="date", isPartition=True),
            ]
        }
    )
    result = SparkReader(routed_spark(qn, catalog=catalog)).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert result.table.partitioned_by == ("p_store", "p_date")


def test_partition_columns_ignores_missing_or_false_flags(qn):
    class NoIsPartition(SimpleNamespace):
        pass

    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                NoIsPartition(name="a", dataType="int", nullable=True, description=""),
                make_catalog_col("b", dataType="string", isPartition=False),
            ]
        }
    )
    result = SparkReader(routed_spark(qn, catalog=catalog)).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert result.table.partitioned_by == ()


def test_fetch_state_lowercases_mixed_case_column_names_from_catalog(qn):
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("CustomerID", dataType="int")]}
    )
    result = SparkReader(routed_spark(qn, catalog=catalog)).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert [c.name for c in result.table.columns] == ["customerid"]


def test_fetch_state_skips_unsupported_column_leaves_mappable_columns_intact(qn):
    catalog = FakeCatalog(
        columns_by_table={
            str(qn): [
                make_catalog_col("id", dataType="int"),
                make_catalog_col("nested", dataType="void"),
            ]
        }
    )
    result = SparkReader(routed_spark(qn, catalog=catalog)).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert [c.name for c in result.table.columns] == ["id"]


def test_fetch_state_returns_failed_when_all_columns_are_unsupported(qn):
    # An ObservedTable requires at least one column; a table whose every column
    # is unmappable cannot be honestly observed, so the read fails.
    catalog = FakeCatalog(columns_by_table={str(qn): [make_catalog_col("nested", dataType="void")]})
    result = SparkReader(routed_spark(qn, catalog=catalog)).fetch_state(qn)

    assert isinstance(result, ReadFailed)


# ---------- table comment ----------


@pytest.mark.parametrize(
    ("desc_value", "expected"),
    [("a comment", "a comment"), (None, "")],
    ids=["set", "unset"],
)
def test_observed_comment_is_description_or_empty_string(qn, desc_value, expected):
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("id", dataType="int")]},
        table_comments={str(qn): desc_value},
    )
    result = SparkReader(routed_spark(qn, catalog=catalog)).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert result.table.comment == expected


# ---------- properties ----------


def test_observed_properties_are_empty_and_read_only_when_table_has_no_properties(qn):
    spark = routed_spark(
        qn, catalog=single_column_catalog(qn), describe=[FakeDetailRow(properties={})]
    )
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TablePresent)
    properties = result.table.properties
    assert dict(properties) == {}
    with pytest.raises(TypeError):
        properties["x"] = "y"  # type: ignore[index]


def test_observed_properties_are_filtered_to_managed_keys(qn):
    describe = [
        FakeDetailRow(
            properties={
                "delta.columnMapping.mode": "name",
                "delta.minReaderVersion": "2",
                "custom.unlisted": "dropped",
            }
        )
    ]
    spark = routed_spark(qn, catalog=single_column_catalog(qn), describe=describe)
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert dict(result.table.properties) == {"delta.columnMapping.mode": "name"}


def test_fetch_state_fails_when_describe_detail_returns_no_rows(qn):
    # DESCRIBE DETAIL yielding no row for a present table is a race or catalog
    # inconsistency, distinct from a table whose properties map is merely empty.
    spark = routed_spark(qn, catalog=single_column_catalog(qn), describe=[])
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, ReadFailed)
    assert result.failure.exception_type == "RuntimeError"


# ---------- totality ----------


def test_fetch_state_returns_absent_when_table_does_not_exist(qn):
    spark = routed_spark(qn, catalog=FakeCatalog(exists=False))
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TableAbsent)
    assert spark.queries == []


def test_fetch_state_returns_failed_when_existence_probe_raises(qn):
    spark = routed_spark(qn, catalog=FakeCatalog(exists_exc=RuntimeError("catalog down")))
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, ReadFailed)
    assert result.failure.exception_type == "RuntimeError"


def test_fetch_state_returns_failed_when_describe_detail_raises(qn):
    spark = routed_spark(qn, catalog=single_column_catalog(qn), describe=AnalysisException("boom"))
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, ReadFailed)
    assert result.failure.exception_type == "AnalysisException"


# ---------- primary key ----------


def test_fetch_state_includes_primary_key_in_observed_table(qn):
    pk_rows = [
        SimpleNamespace(constraint_name="pk_t", column_name="id"),
    ]
    spark = routed_spark(qn, catalog=single_column_catalog(qn), pk=pk_rows)
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert result.table.primary_key == PrimaryKeyConstraint(columns=("id",), constraint_name="pk_t")


# ---------- foreign keys ----------


def test_fetch_state_includes_single_column_foreign_key_in_observed_table(qn):
    # ObservedTable requires a foreign key's local columns to exist among the
    # table's own columns, so this table declares customer_id rather than the
    # single_column_catalog default of "id".
    catalog = FakeCatalog(
        columns_by_table={str(qn): [make_catalog_col("customer_id", dataType="int")]}
    )
    spark = routed_spark(qn, catalog=catalog, fks=[fk_row()])
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert result.table.foreign_keys == (
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=QualifiedName("c", "s", "customers"),
            referenced_columns=("id",),
            constraint_name="fk_orders_customers",
        ),
    )


# ---------- referencing foreign keys ----------


def test_fetch_state_includes_referencing_foreign_key_in_observed_table(qn):
    spark = routed_spark(
        qn, catalog=single_column_catalog(qn), referencing_fks=[referencing_fk_row()]
    )
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert result.table.referencing_foreign_keys == (
        ForeignKeyReference(
            constraint_name="orders_customer_fk",
            referencing_table=QualifiedName("c", "s", "orders"),
        ),
    )


# ---------- tags ----------


def test_fetch_state_observes_table_tags(qn):
    tag_rows = [SimpleNamespace(tag_name="Owner", tag_value="data-platform")]
    spark = routed_spark(qn, catalog=single_column_catalog(qn), tags=tag_rows)
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert dict(result.table.tags) == {"Owner": "data-platform"}


def test_fetch_state_attaches_column_tags_to_their_columns(qn):
    column_tag_rows = [SimpleNamespace(column_name="ID", tag_name="key", tag_value="primary")]
    spark = routed_spark(qn, catalog=single_column_catalog(qn), column_tags=column_tag_rows)
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TablePresent)
    (column,) = result.table.columns
    assert dict(column.tags) == {"key": "primary"}


# ---------- information_schema availability ----------


def test_fetch_state_skips_metadata_queries_when_information_schema_is_absent(qn):
    # Plain Spark (no Unity Catalog): the probe fails once, and no
    # information_schema query is ever issued -- constraints and tags read as
    # empty rather than failing the table.
    spark = routed_spark(
        qn,
        catalog=single_column_catalog(qn),
        probe=AnalysisException(
            message="no information_schema",
            errorClass="TABLE_OR_VIEW_NOT_FOUND",
            messageParameters={},
        ),
    )
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, TablePresent)
    assert result.table.primary_key is None
    assert result.table.foreign_keys == ()
    assert result.table.referencing_foreign_keys == ()
    assert dict(result.table.tags) == {}
    issued = set(spark.queries)
    assert primary_key_query(qn) not in issued
    assert foreign_keys_query(qn) not in issued
    assert referencing_foreign_keys_query(qn) not in issued
    assert table_tags_query(qn) not in issued
    assert column_tags_query(qn) not in issued


@pytest.mark.parametrize(
    "probe_error",
    [
        AnalysisException(
            message="permission denied",
            errorClass="INSUFFICIENT_PERMISSIONS",
            messageParameters={},
        ),
        AnalysisException("boom"),
    ],
    ids=["permission-error", "no-error-condition"],
)
def test_fetch_state_fails_when_the_probe_raises_an_unexpected_error(qn, probe_error):
    # Only the missing-view conditions mean "no information_schema". Anything
    # else (a permission error, an exception with no condition at all) must
    # surface as ReadFailed rather than silently reading the whole catalog as
    # constraint- and tag-free.
    spark = routed_spark(qn, catalog=single_column_catalog(qn), probe=probe_error)
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, ReadFailed)
    assert result.failure.exception_type == "AnalysisException"


def test_probe_availability_is_not_cached_when_the_probe_fails_unexpectedly(qn):
    # A transient probe failure must not poison the catalog for the reader's
    # lifetime: the next read probes again instead of trusting a failed answer.
    spark = routed_spark(
        qn,
        catalog=single_column_catalog(qn),
        probe=AnalysisException(
            message="permission denied",
            errorClass="INSUFFICIENT_PERMISSIONS",
            messageParameters={},
        ),
    )
    reader = SparkReader(spark)
    assert isinstance(reader.fetch_state(qn), ReadFailed)
    assert isinstance(reader.fetch_state(qn), ReadFailed)

    probe = information_schema_probe_query(qn.catalog)
    assert spark.queries.count(probe) == 2


def test_fetch_state_fails_when_a_metadata_query_fails_on_unity_catalog(qn):
    # The probe succeeded, so information_schema exists: a failing metadata
    # query (permissions, query regression) must surface as ReadFailed, not
    # masquerade as "no constraints, no tags".
    spark = routed_spark(
        qn,
        catalog=single_column_catalog(qn),
        fks=AnalysisException("PERMISSION_DENIED"),
    )
    result = SparkReader(spark).fetch_state(qn)

    assert isinstance(result, ReadFailed)
    assert result.failure.exception_type == "AnalysisException"


def test_information_schema_probe_runs_once_per_catalog_across_reads(qn):
    spark = routed_spark(qn, catalog=single_column_catalog(qn))
    reader = SparkReader(spark)
    reader.fetch_state(qn)
    reader.fetch_state(qn)

    probe = information_schema_probe_query(qn.catalog)
    assert spark.queries.count(probe) == 1
