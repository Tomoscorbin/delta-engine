from __future__ import annotations

from collections.abc import Sequence

from delta_engine.application.registry import Registry
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import DataType, Int64, String
from delta_engine.domain.model.identifier import Identifier
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.model.table import DesiredTable, ObservedTable, TableSnapshot


def identifier(value: str) -> Identifier:
    return Identifier(value)

def qualified_name(catalog: str | None, schema: str | None, name: str) -> QualifiedName:
    return QualifiedName(catalog=catalog, schema=schema, name=name)

def columns(specs: Sequence[tuple[str, DataType]]) -> tuple[Column, ...]:
    return tuple(Column(name=n, data_type=t) for n, t in specs)

def table_snapshot(qn: QualifiedName, cols: tuple[Column, ...]) -> TableSnapshot:
    return TableSnapshot(qualified_name=qn, columns=cols)

def desired_table(qn: QualifiedName, cols: tuple[Column, ...]) -> DesiredTable:
    return DesiredTable(qualified_name=qn, columns=cols)

def observed_table(qn: QualifiedName, cols: tuple[Column, ...], is_empty: bool) -> ObservedTable:
    return ObservedTable(qualified_name=qn, columns=cols, is_empty=is_empty)

def default_columns() -> tuple[Column, ...]:
    return columns([("id", Int64()), ("name", String())])

def customers_qn() -> QualifiedName:
    return qualified_name("core", "gold", "customers")

def make_registry(names):
    r = Registry()
    for n in names:
        r.register(DesiredTable(QualifiedName(catalog="c", schema="s", name=n)))
    return r
