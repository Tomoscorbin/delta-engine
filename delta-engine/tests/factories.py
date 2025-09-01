from __future__ import annotations

from collections.abc import Iterable, Sequence

from delta_engine.application.registry import Registry
from delta_engine.adapters.schema.delta.table import DeltaTable
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import DataType, Integer, String
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.model.table import DesiredTable, ObservedTable, TableSnapshot


def make_qualified_name(catalog: str, schema: str, name: str) -> QualifiedName:
    return QualifiedName(catalog=catalog, schema=schema, name=name)


def make_column(
    name: str, dtype: DataType, *, is_nullable: bool = True, comment: str = ""
) -> Column:
    return Column(name=name, data_type=dtype, is_nullable=is_nullable, comment=comment)


def make_columns(specs: Sequence[tuple[str, DataType]]) -> tuple[Column, ...]:
    return tuple(make_column(n, t) for n, t in specs)


def make_table_snapshot(qn: QualifiedName, cols: tuple[Column, ...]) -> TableSnapshot:
    return TableSnapshot(qualified_name=qn, columns=cols)


def make_desired_table(qn: QualifiedName, cols: tuple[Column, ...]) -> DesiredTable:
    return DesiredTable(qualified_name=qn, columns=cols)


def make_observed_table(qn: QualifiedName, cols: tuple[Column, ...]) -> ObservedTable:
    return ObservedTable(qualified_name=qn, columns=cols)


def make_default_columns() -> tuple[Column, ...]:
    return make_columns([("id", Integer()), ("name", String())])


def make_desired_default(name: str, columns: Iterable[Column]) -> DesiredTable:
    qn = QualifiedName(catalog="dev", schema="silver", name=name)
    return DesiredTable(qualified_name=qn, columns=tuple(columns))


def make_observed_default(name: str, columns: Iterable[Column]) -> ObservedTable:
    qn = QualifiedName(catalog="dev", schema="silver", name=name)
    return ObservedTable(qualified_name=qn, columns=tuple(columns))


def make_registry(names):
    """Build a Registry from simple names using real table specs.

    Uses `DeltaTable` with a minimal default schema (`id` INT) under
    the `dev.silver` namespace to align with Registry expectations.
    """
    r = Registry()
    for n in names:
        r.register(
            DeltaTable(
                "dev",
                "silver",
                n,
                (make_column("id", Integer(), is_nullable=False),),
            )
        )
    return r
