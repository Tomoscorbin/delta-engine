from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
import pytest

from tabula.adapters.databricks.catalog.reader import UCReader
from tabula.domain.model.data_type import DataType
from tabula.domain.model.column import Column

# --- minimal domain double for QualifiedName ---------------------------------
@dataclass(frozen=True)
class _QN:
    catalog: str | None
    schema: str | None
    name: str

    @property
    def dotted(self) -> str:
        parts = [p for p in (self.catalog, self.schema, self.name) if p]
        return ".".join(parts)

# --- spark stubs -------------------------------------------------------------
class _FakeCatalog:
    def __init__(self, exists: bool, columns: list[object]):
        self._exists = exists
        self._columns = columns
        self._last_exists_arg = None
        self._last_list_arg = None

    def tableExists(self, name: str) -> bool:
        self._last_exists_arg = name
        return self._exists

    def listColumns(self, name: str):
        self._last_list_arg = name
        return list(self._columns)

class _FakeDF:
    def __init__(self, is_empty: bool):
        self._empty = is_empty
    def isEmpty(self) -> bool:
        return self._empty

class _FakeSpark:
    def __init__(self, exists: bool, columns: list[object], is_empty: bool):
        self.catalog = _FakeCatalog(exists, columns)
        self._is_empty = is_empty
    def table(self, name: str) -> _FakeDF:
        return _FakeDF(self._is_empty)

# --- tests -------------------------------------------------------------------

def test_fetch_state_returns_none_when_table_missing(monkeypatch) -> None:
    # No need to patch type mapping; it won't be called.
    spark = _FakeSpark(exists=False, columns=[], is_empty=True)
    reader = UCReader(spark)

    qn = _QN("c", "s", "t")
    result = reader.fetch_state(qn)

    assert result is None
    assert spark.catalog._last_exists_arg == "c.s.t"

def test_fetch_state_returns_observed_table_with_columns_and_empty_flag(monkeypatch) -> None:
    # Patch type mapping for deterministic domain DataType
    from tabula.adapters.databricks.catalog import reader as reader_mod
    monkeypatch.setattr(reader_mod, "domain_type_from_spark",
                        lambda spark_dt: DataType("int") if spark_dt == "int" else DataType("string"))

    # Spark columns stub (shape like pyspark.sql.catalog.Column)
    spark_cols = [
        SimpleNamespace(name="id",   dataType="int",    nullable=False),
        SimpleNamespace(name="note", dataType="string", nullable=True),
    ]
    spark = _FakeSpark(exists=True, columns=spark_cols, is_empty=False)
    reader = UCReader(spark)

    qn = _QN("c", "s", "t")
    observed = reader.fetch_state(qn)

    assert observed is not None
    assert observed.qualified_name is qn
    assert observed.is_empty is False

    # Columns mapped correctly and in order
    assert observed.columns == (
        Column(name="id",   data_type=DataType("int"),    is_nullable=False),
        Column(name="note", data_type=DataType("string"), is_nullable=True),
    )

    # Called the catalog with dotted name
    assert spark.catalog._last_exists_arg == "c.s.t"
    assert spark.catalog._last_list_arg == "c.s.t"

def test_is_table_empty_true(monkeypatch) -> None:
    # Patch mapping to avoid errors if called
    from tabula.adapters.databricks.catalog import reader as reader_mod
    monkeypatch.setattr(reader_mod, "domain_type_from_spark", lambda _: DataType("string"))

    spark_cols = [SimpleNamespace(name="x", dataType="string", nullable=True)]
    spark = _FakeSpark(exists=True, columns=spark_cols, is_empty=True)
    reader = UCReader(spark)

    qn = _QN(None, "schema", "tbl")
    observed = reader.fetch_state(qn)

    assert observed is not None
    assert observed.is_empty is True
