from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from delta_engine.application.ports import ColumnObject
from delta_engine.application.registry import Registry
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import DataType, Integer, String
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.model.table import DesiredTable, ObservedTable, TableSnapshot


def make_qualified_name(catalog: str, schema: str, name: str) -> QualifiedName:
    return QualifiedName(catalog=catalog, schema=schema, name=name)


def make_columns(specs: Sequence[tuple[str, DataType]]) -> tuple[Column, ...]:
    return tuple(Column(name=n, data_type=t) for n, t in specs)


def make_table_snapshot(qn: QualifiedName, cols: tuple[Column, ...]) -> TableSnapshot:
    return TableSnapshot(qualified_name=qn, columns=cols)


def make_desired_table(qn: QualifiedName, cols: tuple[Column, ...]) -> DesiredTable:
    return DesiredTable(qualified_name=qn, columns=cols)


def make_observed_table(qn: QualifiedName, cols: tuple[Column, ...]) -> ObservedTable:
    return ObservedTable(qualified_name=qn, columns=cols)


def make_default_columns() -> tuple[Column, ...]:
    return make_columns([("id", Integer()), ("name", String())])


def make_registry(names):
    r = Registry()
    for n in names:
        r.register(DesiredTable(QualifiedName(catalog="c", schema="s", name=n)))
    return r


def make_table_spec(
    catalog: str,
    schema: str,
    name: str,
    columns: Iterable[ColumnSpec],
    properties: Mapping[str, str] | None = None,
) -> TableSpec:
    return TableSpec(
        catalog=catalog,
        schema=schema,
        name=name,
        columns=columns,
        properties=properties,
    )


@dataclass
class ColumnSpec:
    """Column spec for testing (duck-types to ColumnObject)."""

    name: str
    data_type: Any
    is_nullable: bool = True


@dataclass
class TableSpec:
    """
    Table spec for testing (duck-types to TableObject).

    This class is not part of the production domain model. It exists only to
    provide test doubles with the same shape as the TableObject protocol.
    """

    catalog: str
    schema: str
    name: str
    columns: Iterable[ColumnObject]
    properties: Mapping[str, str] | None = None
