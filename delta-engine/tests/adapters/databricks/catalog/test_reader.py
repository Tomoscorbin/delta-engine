from __future__ import annotations

from types import MappingProxyType, SimpleNamespace

import pytest

from delta_engine.adapters.databricks.catalog.reader import DatabricksReader
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.model.table import ObservedTable
from tests.factories import make_qualified_name

# --- tiny spark stubs ---------------------------------------------------------


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
        self, name: str, data_type: object, nullable: bool = True, description: str | None = None
    ):
        self.name = name
        self.dataType = data_type
        self.nullable = nullable
        self.description = description


# --- fixtures -----------------------------------------------------------------


@pytest.fixture
def qn():
    return make_qualified_name("dev", "silver", "people")


# --- helpers for hijacking result construction (keep tests decoupled) ---------


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


# --- tests --------------------------------------------------------------------


def test_fetch_state_returns_absent_when_table_missing(monkeypatch: pytest.MonkeyPatch, qn) -> None:
    # table existence query -> empty result => no table
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_table_existence",
        lambda _qn: "EXISTS",
    )
    spark = StubSpark(sql_map={"EXISTS": StubDF(head_values=[])}, catalog=StubCatalog(columns=[]))

    # hijack result construction to avoid coupling to application layer details
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.ReadResult", FakeReadResult
    )

    reader = DatabricksReader(spark)
    result = reader.fetch_state(qn)

    assert isinstance(result, FakeReadResult)
    assert result.kind == "absent"


def test_fetch_state_returns_present_with_columns_and_properties(
    monkeypatch: pytest.MonkeyPatch, qn
) -> None:
    # Existence -> present
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_table_existence",
        lambda _qn: "EXISTS",
    )
    # Describe detail returns a row with properties
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_describe_detail",
        lambda _qn: "DETAIL",
    )
    # Map Spark dtype -> domain dtype (keep it trivial)
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.domain_type_from_spark",
        lambda _dt: Integer(),
    )
    # Enforce policy returns an immutable mapping (simulate enforcement)
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.enforce_property_policy",
        lambda props: MappingProxyType(dict(props)),
    )
    # Fake spark
    df_exists = StubDF(head_values=[1])
    props_row = StubRow({"owner": "asda", "quality": "gold"})
    df_detail = StubDF(first_row=props_row)
    spark_cols = [
        SparkCol("id", data_type=object(), nullable=True, description=None),
        SparkCol("age", data_type=object(), nullable=False, description="years"),
    ]
    spark = StubSpark(
        sql_map={"EXISTS": df_exists, "DETAIL": df_detail}, catalog=StubCatalog(columns=spark_cols)
    )

    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.ReadResult", FakeReadResult
    )

    reader = DatabricksReader(spark)
    result = reader.fetch_state(qn)

    assert result.kind == "present"
    observed = result.payload
    assert isinstance(observed, ObservedTable)
    # columns
    names = [c.name for c in observed.columns]
    assert set(names) == {"id", "age"}
    age = next(c for c in observed.columns if c.name == "age")
    assert isinstance(age.data_type, Integer)
    assert age.is_nullable is False
    assert age.comment == "years"
    # properties are immutable and contain policy-enforced values
    assert dict(observed.properties) == {"owner": "asda", "quality": "gold"}
    with pytest.raises(TypeError):
        observed.properties["owner"] = "someone-else"  # type: ignore[index]


def test_fetch_state_failure_wraps_exception_with_preview(
    monkeypatch: pytest.MonkeyPatch, qn
) -> None:
    # Table exists
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_table_existence",
        lambda _qn: "EXISTS",
    )
    spark = StubSpark(
        sql_map={"EXISTS": StubDF(head_values=[1])},
        catalog=StubCatalog(columns=[], raise_on_list=ValueError("boom")),
    )
    # error preview stub + failure/result stubs
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.error_preview", lambda exc: "PREVIEW"
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.ReadResult", FakeReadResult
    )
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.ReadFailure", FakeReadFailure
    )

    reader = DatabricksReader(spark)
    result = reader.fetch_state(qn)

    assert result.kind == "failed"
    failure = result.payload
    assert isinstance(failure, FakeReadFailure)
    assert failure.error_type == "ValueError"
    assert failure.preview == "PREVIEW"


def test_fetch_properties_returns_empty_when_describe_detail_has_no_rows(
    monkeypatch: pytest.MonkeyPatch, qn
) -> None:
    # No row returned -> empty MappingProxyType
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.query_describe_detail",
        lambda _qn: "DETAIL",
    )
    spark = StubSpark(sql_map={"DETAIL": StubDF(first_row=None)}, catalog=StubCatalog(columns=[]))
    reader = DatabricksReader(spark)

    props = reader._fetch_properties(qn)

    assert isinstance(props, MappingProxyType)
    assert dict(props) == {}
    with pytest.raises(TypeError):
        props["x"] = "y"  # type: ignore[index]


def test_to_domain_column_maps_fields_and_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "delta_engine.adapters.databricks.catalog.reader.domain_type_from_spark",
        lambda _dt: Integer(),
    )
    reader = DatabricksReader(StubSpark(sql_map={}, catalog=StubCatalog(columns=[])))

    # nullable True, no description -> empty comment
    sc1 = SparkCol("col1", data_type=object(), nullable=True, description=None)
    c1 = reader._to_domain_column(sc1)
    assert isinstance(c1, Column)
    assert c1.name == "col1"
    assert isinstance(c1.data_type, Integer)
    assert c1.is_nullable is True
    assert c1.comment == ""

    # nullable False, with description
    sc2 = SparkCol("col2", data_type=object(), nullable=False, description="hello")
    c2 = reader._to_domain_column(sc2)
    assert c2.is_nullable is False
    assert c2.comment == "hello"


def test_fetch_table_comment_returns_description_or_empty() -> None:
    spark_with_desc = StubSpark(
        sql_map={}, catalog=StubCatalog(columns=[], table_description="note")
    )
    spark_without_desc = StubSpark(
        sql_map={}, catalog=StubCatalog(columns=[], table_description=None)
    )
    reader1 = DatabricksReader(spark_with_desc)
    reader2 = DatabricksReader(spark_without_desc)

    assert reader1._fetch_table_comment("dev.silver.people") == "note"
    assert reader2._fetch_table_comment("dev.silver.people") == ""
