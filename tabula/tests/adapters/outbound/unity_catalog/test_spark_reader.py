from dataclasses import dataclass

from tabula.adapters.databricks.catalog.spark_reader import UCSparkReader
from tabula.adapters.databricks.sql.compile import quote_ident, table_ref
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.table import ObservedTable


@dataclass
class _ColInfo:
    name: str
    dataType: str
    nullable: bool


class _FakeTable:
    def __init__(self, is_empty: bool) -> None:
        self._empty = is_empty

    def isEmpty(self) -> bool:
        return self._empty


class _FakeCatalog:
    def __init__(self, exists: bool, cols: list[_ColInfo]) -> None:
        self._exists = exists
        self._cols = cols

    def tableExists(self, fqn: str) -> bool:
        return self._exists

    def listColumns(self, fqn: str) -> list[_ColInfo]:
        return self._cols


class _FakeSpark:
    def __init__(self, exists: bool, cols: list[_ColInfo], is_empty: bool) -> None:
        self.catalog = _FakeCatalog(exists, cols)
        self._table = _FakeTable(is_empty)

    def table(self, _ident: str) -> _FakeTable:
        return self._table


def qn() -> QualifiedName:
    return QualifiedName("c", "s", "t")


def test_quote_ident_escapes_backticks_and_wraps():
    assert quote_ident("plain") == "`plain`"
    assert quote_ident("we`ird") == "`we``ird`"


def test_table_ref_backticks_and_escapes_all_parts():
    qn = QualifiedName("Cat`alog", "Sch`ema", "Na`me")
    # QualifiedName should case-fold to lowercase
    assert table_ref(qn) == "`cat``alog`.`sch``ema`.`na``me`"



def test_table_exists_helper_true_and_false():
    cols = [_ColInfo("id", "int", False)]
    reader_true = UnityCatalogSparkReader(spark=_FakeSpark(True, cols, True))  # type: ignore[arg-type]
    reader_false = UnityCatalogSparkReader(spark=_FakeSpark(False, [], True))  # type: ignore[arg-type]
    assert reader_true._table_exists(qn()) is True
    assert reader_false._table_exists(qn()) is False


def test_list_columns_helper_maps_types_and_nullable():
    cols = [
        _ColInfo("id", "int", False),
        _ColInfo("name", "string", True),
    ]
    reader = UnityCatalogSparkReader(spark=_FakeSpark(True, cols, True))  # type: ignore[arg-type]
    result = reader._list_columns(qn())
    assert tuple(c.name for c in result) == ("id", "name")
    assert result[0].data_type.name == "integer"  # int -> integer
    assert result[1].is_nullable is True


def test_is_table_empty_helper_true_and_false():
    reader_empty = UnityCatalogSparkReader(spark=_FakeSpark(True, [], True))    # type: ignore[arg-type]
    reader_nonempty = UnityCatalogSparkReader(spark=_FakeSpark(True, [], False))  # type: ignore[arg-type]
    assert reader_empty._is_table_empty(qn()) is True
    assert reader_nonempty._is_table_empty(qn()) is False


def test_fetch_state_end_to_end_when_exists_and_empty():
    cols = [_ColInfo("id", "int", False)]
    reader = UnityCatalogSparkReader(spark=_FakeSpark(True, cols, True))  # type: ignore[arg-type]
    observed = reader.fetch_state(qn())
    assert isinstance(observed, ObservedTable)
    assert observed.is_empty is True
    assert tuple(c.name for c in observed.columns) == ("id",)


def test_fetch_state_returns_none_when_not_exists():
    reader = UnityCatalogSparkReader(spark=_FakeSpark(False, [], True))  # type: ignore[arg-type]
    assert reader.fetch_state(qn()) is None
