"""
Adopt catalog column spellings into desired tables before diffing.

Identity is case-insensitive (``Identifier``), but Databricks resolves the
column names inside ``ADD CONSTRAINT`` statements case-sensitively — on both
the local and the referenced side — so constraint SQL must spell columns
exactly as the catalog does. Running this adoption once, before diffing,
means every later stage can emit desired values verbatim: a column that
already exists keeps the catalog's spelling, and a new column keeps its
declared spelling.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace

from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ObservedTable,
    QualifiedName,
)
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint

type _SpellingMap = Mapping[str, str]


def adopt_catalog_spellings(
    pairs: Iterable[tuple[DesiredTable, ObservedTable | None]],
) -> dict[QualifiedName, DesiredTable]:
    """
    Respell each desired table with the catalog's column spellings.

    Every column reference — column names, primary-key and foreign-key
    columns, partition and clustering keys — adopts the observed spelling
    where the column already exists and keeps the declared spelling where it
    does not. Foreign-key referenced columns adopt the *referenced* table's
    spelling; a reference to a table absent from ``pairs`` keeps its declared
    spelling (dependency resolution rejects such runs before execution).
    ``renamed_from`` hints are left untouched: the rename's old spelling
    comes from the observed column, and its new spelling is by definition
    not in the catalog yet.
    """
    pair_list = tuple(pairs)
    spellings_by_table = {
        desired.qualified_name: _spellings(desired, observed) for desired, observed in pair_list
    }
    return {
        desired.qualified_name: _respell_table(desired, spellings_by_table)
        for desired, _ in pair_list
    }


def _spellings(desired: DesiredTable, observed: ObservedTable | None) -> _SpellingMap:
    """Effective spelling per column: the catalog's where present, else the declared."""
    declared: dict[str, str] = {column.name: column.name for column in desired.columns}
    if observed is None:
        return declared
    return declared | {column.name: column.name for column in observed.columns}


def _respell_table(
    desired: DesiredTable,
    spellings_by_table: Mapping[QualifiedName, _SpellingMap],
) -> DesiredTable:
    own = spellings_by_table[desired.qualified_name]
    return replace(
        desired,
        columns=tuple(_respell_column(column, own) for column in desired.columns),
        partitioned_by=tuple(own.get(name, name) for name in desired.partitioned_by),
        clustered_by=tuple(own.get(name, name) for name in desired.clustered_by),
        primary_key=_respell_primary_key(desired.primary_key, own),
        foreign_keys=tuple(
            _respell_foreign_key(constraint, own, spellings_by_table)
            for constraint in desired.foreign_keys
        ),
    )


def _respell_column(column: DesiredColumn, spellings: _SpellingMap) -> DesiredColumn:
    return replace(column, name=spellings.get(column.name, column.name))


def _respell_primary_key(
    primary_key: PrimaryKeyConstraint | None,
    spellings: _SpellingMap,
) -> PrimaryKeyConstraint | None:
    if primary_key is None:
        return None
    return replace(
        primary_key,
        columns=tuple(spellings.get(name, name) for name in primary_key.columns),
    )


def _respell_foreign_key(
    constraint: ForeignKeyConstraint,
    local_spellings: _SpellingMap,
    spellings_by_table: Mapping[QualifiedName, _SpellingMap],
) -> ForeignKeyConstraint:
    referenced_spellings = spellings_by_table.get(constraint.referenced_table, {})
    return replace(
        constraint,
        local_columns=tuple(local_spellings.get(name, name) for name in constraint.local_columns),
        referenced_columns=tuple(
            referenced_spellings.get(name, name) for name in constraint.referenced_columns
        ),
    )
