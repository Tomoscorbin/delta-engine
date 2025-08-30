from dataclasses import dataclass

from delta_engine.adapters.databricks.catalog.reader import DatabricksReader
from delta_engine.application.results import ReadResult
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from tests.factories import make_qualified_name

# --- stubs ----------------------------------------------------------------
_QN = make_qualified_name("dev", "silver", "people")


@dataclass
class _StubCol:
    name: str
    dataType: object
    nullable: bool = True


class _StubCatalog:
    def __init__(self, *, exists: bool = True, columns: list[_StubCol] | None = None):
        self._exists = exists
        self._columns = columns or []

    def tableExists(self, fqn: str) -> bool:
        return self._exists

    def listColumns(self, fqn: str):
        return list(self._columns)


class _StubSpark:
    def __init__(self, catalog):
        self.catalog = catalog


class _CatalogRaisingOnList(_StubCatalog):
    def __init__(self, exc: Exception):
        super().__init__(exists=True)
        self._exc = exc

    def listColumns(self, fqn: str):
        raise self._exc


class _CatalogRaisingOnExists(_StubCatalog):
    def __init__(self, exc: Exception):
        super().__init__(exists=True)
        self._exc = exc

    def tableExists(self, fqn: str) -> bool:
        raise self._exc


# --- tests ---------------------------------------------------------------------


def test_reader_present_maps_columns_and_types(spark) -> None:
    cols = [
        _StubCol("id", "int", False),
        _StubCol("name", "string", True),
    ]
    spark = _StubSpark(_StubCatalog(exists=True, columns=cols))
    reader = DatabricksReader(spark)

    res = reader.fetch_state(_QN)

    assert isinstance(res, ReadResult)
    assert res.failed is False
    assert res.observed is not None
    assert res.observed.qualified_name == _QN
    assert tuple(isinstance(c, Column) for c in res.observed.columns) == (True, True)
    # type mapping via pure-parse path
    assert res.observed.columns[0].data_type == Integer()
    assert res.observed.columns[1].data_type == String()
    assert res.observed.columns[0].is_nullable is False
    assert res.observed.columns[1].is_nullable is True


def test_reader_absent_via_table_exists_false() -> None:
    spark = _StubSpark(_StubCatalog(exists=False, columns=[]))
    reader = DatabricksReader(spark)

    res = reader.fetch_state(_QN)

    assert res.failed is False
    assert res.observed is None  # absent


def test_reader_failed_when_any_other_exception() -> None:
    spark = _StubSpark(
        _CatalogRaisingOnList(RuntimeError("boom\nline2\nline3\nline4\nline5\nline6"))
    )
    reader = DatabricksReader(spark)

    res = reader.fetch_state(_QN)

    assert res.failed is True
    assert res.failure is not None
    assert res.failure.exception_type == "RuntimeError"
    # error_preview returns first 5 lines; at least ensure message contains the head
    assert "boom" in res.failure.message
