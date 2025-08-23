from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from tabula.adapters.databricks.catalog.reader import UCReader
from tabula.domain.model import Column, DataType

# ---------- minimal QualifiedName double --------------------------------------


@dataclass(frozen=True)
class _QN:
    catalog: str | None
    schema: str | None
    name: str

    @property
    def dotted(self) -> str:
        parts = [p for p in (self.catalog, self.schema, self.name) if p]
        return ".".join(parts)


# ---------- Spark stubs -------------------------------------------------------


class _FakeCatalog:
    def __init__(self, exists: bool, columns: list[object]):
        self._exists = exists
        self._columns = columns
        self._last_exists_arg: str | None = None
        self._last_list_arg: str | None = None

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
        self._last_table_arg: str | None = None

    def table(self, name: str) -> _FakeDF:
        self._last_table_arg = name
        return _FakeDF(self._is_empty)


# ---------- tests -------------------------------------------------------------


def test_missing_table_returns_none_and_does_not_probe(monkeypatch) -> None:
    spark = _FakeSpark(exists=False, columns=[], is_empty=True)
    reader = UCReader(spark)

    qn = _QN("c", "s", "t")
    result = reader.fetch_state(qn)

    assert result is None
    assert spark.catalog._last_exists_arg == "c.s.t"
    assert spark.catalog._last_list_arg is None
    assert spark._last_table_arg is None


def test_maps_columns_and_checks_empty(monkeypatch) -> None:
    # Patch the mapper at the module UCReader uses.
    from tabula.adapters.databricks.catalog import reader as reader_mod

    monkeypatch.setattr(reader_mod, "domain_type_from_spark", lambda dt: DataType(dt))

    spark_cols = [
        SimpleNamespace(name="id", dataType="int", nullable=False),
        SimpleNamespace(name="note", dataType="string", nullable=True),
    ]
    spark = _FakeSpark(exists=True, columns=spark_cols, is_empty=False)
    reader = UCReader(spark)

    qn = _QN("c", "s", "t")
    observed = reader.fetch_state(qn)

    assert observed is not None
    assert observed.qualified_name is qn
    assert observed.is_empty is False
    assert observed.columns == (
        Column(name="id", data_type=DataType("int"), is_nullable=False),
        Column(name="note", data_type=DataType("string"), is_nullable=True),
    )
    assert spark.catalog._last_exists_arg == "c.s.t"
    assert spark.catalog._last_list_arg == "c.s.t"
    assert spark._last_table_arg == "c.s.t"


def test_is_empty_true_path(monkeypatch) -> None:
    from tabula.adapters.databricks.catalog import reader as reader_mod

    monkeypatch.setattr(reader_mod, "domain_type_from_spark", lambda dt: DataType(dt))

    spark_cols = [SimpleNamespace(name="x", dataType="string", nullable=True)]
    spark = _FakeSpark(exists=True, columns=spark_cols, is_empty=True)
    reader = UCReader(spark)

    observed = reader.fetch_state(_QN(None, "schema", "tbl"))
    assert observed is not None
    assert observed.is_empty is True
    # Dotted name omitted catalog cleanly
    assert spark.catalog._last_exists_arg == "schema.tbl"
    assert spark.catalog._last_list_arg == "schema.tbl"
    assert spark._last_table_arg == "schema.tbl"


def test_type_mapping_exception_bubbles(monkeypatch) -> None:
    from tabula.adapters.databricks.catalog import reader as reader_mod

    def _boom(_):
        raise ValueError("unsupported spark type")

    monkeypatch.setattr(reader_mod, "domain_type_from_spark", _boom)

    spark_cols = [SimpleNamespace(name="id", dataType="struct<weird:stuff>", nullable=True)]
    spark = _FakeSpark(exists=True, columns=spark_cols, is_empty=False)
    reader = UCReader(spark)

    with pytest.raises(ValueError, match="unsupported spark type"):
        reader.fetch_state(_QN("c", "s", "t"))


def test_table_isEmpty_exception_bubbles(monkeypatch) -> None:
    from tabula.adapters.databricks.catalog import reader as reader_mod

    monkeypatch.setattr(reader_mod, "domain_type_from_spark", lambda dt: DataType(dt))

    class _BoomSpark(_FakeSpark):
        def table(self, name: str):
            self._last_table_arg = name
            raise RuntimeError("I/O error reading table")

    spark_cols = [SimpleNamespace(name="id", dataType="int", nullable=False)]
    spark = _BoomSpark(exists=True, columns=spark_cols, is_empty=False)
    reader = UCReader(spark)

    with pytest.raises(RuntimeError, match="I/O error"):
        reader.fetch_state(_QN("c", "s", "t"))

    # We still performed existence + column listing before failing
    assert spark.catalog._last_exists_arg == "c.s.t"
    assert spark.catalog._last_list_arg == "c.s.t"
    assert spark._last_table_arg == "c.s.t"


def test_listcolumns_iterable_supported_and_order_preserved(monkeypatch) -> None:
    from tabula.adapters.databricks.catalog import reader as reader_mod

    monkeypatch.setattr(reader_mod, "domain_type_from_spark", lambda dt: DataType(dt))

    # Supply columns in a specific order
    spark_cols = [
        SimpleNamespace(name="b", dataType="int", nullable=False),
        SimpleNamespace(name="a", dataType="string", nullable=True),
    ]
    spark = _FakeSpark(exists=True, columns=spark_cols, is_empty=False)
    reader = UCReader(spark)

    observed = reader.fetch_state(_QN("c", None, "t"))
    assert observed is not None
    assert observed.columns == (
        Column(name="b", data_type=DataType("int"), is_nullable=False),
        Column(name="a", data_type=DataType("string"), is_nullable=True),
    )
