from typing import Callable
import pytest
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.model.types import integer


# factory helpers
from tabula.domain.model.qualified_name import QualifiedName

@pytest.fixture
def make_qn() -> Callable[[str, str, str], QualifiedName]:
    def _make_qn(
        catalog: str = "cat",
        schema: str = "sch",
        name: str = "tbl",
    ) -> QualifiedName:
        return QualifiedName(catalog, schema, name)
    return _make_qn


def col(name: str, dt: DataType = None, nullable: bool = True) -> Column:
    return Column(name=name, data_type=dt or integer(), is_nullable=nullable)


def desired(qualified_name: QualifiedName, *columns: Column) -> DesiredTable:
    return DesiredTable(qualified_name=qualified_name, columns=tuple(columns))


def observed(qualified_name: QualifiedName, *columns: Column) -> ObservedTable:
    return ObservedTable(
        qualified_name=qualified_name, 
        columns=tuple(columns),
        is_empty= False
    )
