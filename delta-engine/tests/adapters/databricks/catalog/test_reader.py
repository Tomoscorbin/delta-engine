# tests/adapters/databricks/catalog/test_reader.py
from __future__ import annotations

from types import MappingProxyType, SimpleNamespace

import pytest

from delta_engine.adapters.databricks.catalog.reader import DatabricksReader
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.model.table import ObservedTable
from tests.factories import make_qualified_name

# ---------- tiny spark stubs (robust defaults) --------------------------------


class StubRow:
    def __init__(self, properties: dict[str, str] | None):
        self._properties = properties

    def __getitem__(self, key: str):
        if key == "properties":
            return self._properties
        raise KeyError(key)


class StubDF:
    def __init__(self, head_values: list | None = None, first_row: StubRow | None = None):
        self._head_values = head_values or []
        self._first_row = first_row

    def head(self, n: int):
        return list(self._head_values)[:n]

    def first(self):
        return self._first_row


class StubCatalog:
    def __init__(
        self,
        columns: list,
        table_description: str | None = None,
        raise_on_list: Exception | None = None,
    ):
        self._columns = columns
        self._table_description = table_description
        self._raise_on_list = raise_on_list

    def listColumns(self, fully_qualified_name: str):
        if self._raise_on_list:
            raise self._raise_on_list
        return list(self._columns)

    def getTable(self, fully_qualified_name: str):
        return SimpleNamespace(description=self._table_description)


class StubSpark:
    def __init__(self, sql_map: dict[str, StubDF], catalog: StubCatalog):
        self._sql_map = sql_map
        self.catalog = catalog

    def sql(self, query: str):
        return self._sql_map[query]


class SparkCol:
    """Minimal shape compatible with pyspark.sql.catalog.Column for our converter."""

    def __init__(
        self,
        name: str,
        data_type: object,
        *,
        nullable: bool = True,
        description: str | None = None,
        isPartition: bool = False,
    ):
        self.name = name
        self.dataType = data_type
        self.nullable = nullable
        self.description = description
        self.isPartition = isPartition  # <- default False for stability


# ---------- fixtures -----------------------------------------------------------


@pytest.fixture
def qn():
    return make_qualified_name("dev", "silver", "people")


# ---------- decoupled result fakes --------------------------------------------


class FakeReadResult:
    def __init__(self, kind: str, payload=None):
        self.kind = kind
        self.payload = payload

    @classmethod
    def create_absent(cls):
        return cls("absent")

    @classmethod
    def create_present(cls, observed):
        return cls("present", observed)

    @classmethod
    def create_failed(cls, failure):
        return cls("failed", failure)


class FakeReadFailure:
    def __init__(self, error_type: str, preview: str):
        self.error_type = error_type
        self.preview = preview


# ---------- small, focused tests ----------------------------------------------


def test_absent_returns_absent(monkeypatch: pytest.MonkeyPatch, qn) -> None:
    # existence query returns no rows -> absent
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_table_existence",
        lambda _qn: "EXISTS",
    )
    spark = StubSpark(sql_map={"EXISTS": StubDF(head_values=[])}, catalog=StubCatalog(columns=[]))
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.ReadResult", FakeReadResult
    )

    result = DatabricksReader(spark).fetch_state(qn)
    assert result.kind == "absent"


def test_present_maps_columns_properties_and_defaults_partitioning(
    monkeypatch: pytest.MonkeyPatch, qn
) -> None:
    # minimal happy path with 2 columns and properties; no partitions by default
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_table_existence",
        lambda _qn: "EXISTS",
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_describe_detail",
        lambda _qn: "DETAIL",
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.domain_type_from_spark",
        lambda _dt: Integer(),
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.enforce_property_policy",
        lambda props: MappingProxyType(dict(props)),
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.ReadResult", FakeReadResult
    )

    df_exists = StubDF(head_values=[1])
    df_detail = StubDF(first_row=StubRow({"owner": "asda", "quality": "gold"}))
    spark_cols = [
        SparkCol("id", data_type=object(), nullable=True, description=None),
        SparkCol("age", data_type=object(), nullable=False, description="years"),
    ]
    spark = StubSpark(
        sql_map={"EXISTS": df_exists, "DETAIL": df_detail}, catalog=StubCatalog(columns=spark_cols)
    )

    result = DatabricksReader(spark).fetch_state(qn)
    assert result.kind == "present"
    observed = result.payload
    assert isinstance(observed, ObservedTable)
    assert {c.name for c in observed.columns} == {"id", "age"}
    assert next(c for c in observed.columns if c.name == "age").comment == "years"
    assert dict(observed.properties) == {"owner": "asda", "quality": "gold"}
    assert observed.partitioned_by == ()  # default: unpartitioned


def test_failure_wraps_exception_with_preview(monkeypatch: pytest.MonkeyPatch, qn) -> None:
    # listColumns raises -> failed with error preview
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_table_existence",
        lambda _qn: "EXISTS",
    )
    spark = StubSpark(
        sql_map={"EXISTS": StubDF(head_values=[1])},
        catalog=StubCatalog(columns=[], raise_on_list=ValueError("boom")),
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.error_preview", lambda exc: "PREVIEW"
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.ReadResult", FakeReadResult
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.ReadFailure", FakeReadFailure
    )

    result = DatabricksReader(spark).fetch_state(qn)
    assert result.kind == "failed"
    failure = result.payload
    assert isinstance(failure, FakeReadFailure)
    assert (failure.error_type, failure.preview) == ("ValueError", "PREVIEW")


def test_fetch_properties_empty_when_describe_detail_has_no_rows(
    monkeypatch: pytest.MonkeyPatch, qn
) -> None:
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_describe_detail",
        lambda _qn: "DETAIL",
    )
    spark = StubSpark(sql_map={"DETAIL": StubDF(first_row=None)}, catalog=StubCatalog(columns=[]))
    props = DatabricksReader(spark)._fetch_properties(qn)
    assert isinstance(props, MappingProxyType)
    assert dict(props) == {}


def test_to_domain_column_basic_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.domain_type_from_spark",
        lambda _dt: Integer(),
    )
    reader = DatabricksReader(StubSpark(sql_map={}, catalog=StubCatalog(columns=[])))

    c1 = reader._to_domain_column(
        SparkCol("col1", data_type=object(), nullable=True, description=None)
    )
    c2 = reader._to_domain_column(
        SparkCol("col2", data_type=object(), nullable=False, description="hello")
    )

    assert (
        isinstance(c1, Column) and c1.name == "col1" and c1.comment == "" and c1.is_nullable is True
    )
    assert c2.name == "col2" and c2.comment == "hello" and c2.is_nullable is False
