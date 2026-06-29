"""
Foreign key dependency resolution for sync ordering and constraint skipping.

The public entry point is `resolve`, which takes the registered tables and
returns a `ForeignKeySyncPlan` containing:

- The tables in dependency-first order (referenced tables before their
  dependents), with cycle members appended at the end.
- The FK constraints that cannot be applied, with their skip reason.
- A per-table index of skipped constraint names, ready for plan filtering.

All graph-traversal implementation details (adjacency map, in-degree tracking,
Kahn's algorithm) are hidden behind that interface.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from delta_engine.application.results import SkipReason, SkippedForeignKey
from delta_engine.domain.model.table import DesiredTable


@dataclass(frozen=True)
class ForeignKeySyncPlan:
    """
    The result of resolving cross-table FK dependencies.

    Attributes:
        ordered_tables: All tables in dependency-first sync order. Cycle
            members are appended after all resolvable tables; their FK
            actions will be stripped before execution.
        skipped_foreign_keys: FK constraints that cannot be applied, with
            their skip reason.
        skipped_names_by_table: Per-table frozenset of constraint names to
            suppress. Passed to plan filtering so the engine does not need
            to rebuild this index.
    """

    ordered_tables: tuple[DesiredTable, ...]
    skipped_foreign_keys: tuple[SkippedForeignKey, ...]
    skipped_names_by_table: dict[str, frozenset[str]]


def resolve(tables: tuple[DesiredTable, ...]) -> ForeignKeySyncPlan:
    """
    Resolve FK dependencies across all registered tables.

    Builds a dependency graph, runs Kahn's topological sort to find the
    safe sync order and detect cycles, then classifies every FK constraint
    as either applicable or skipped (CYCLE / UNRESOLVABLE_REFERENCE).

    Args:
        tables: All desired tables from the registry, in registry order.

    Returns:
        A :class:`ForeignKeySyncPlan` with ordered tables, skipped FKs,
        and a per-table lookup of constraint names to suppress.
    """
    registered_names = {str(table.qualified_name) for table in tables}
    graph = _build_dependency_graph(tables, registered_names)
    ordered, cycle_members = _toposort(tables, graph)

    all_tables_in_order = ordered + [
        table for table in tables if str(table.qualified_name) in cycle_members
    ]

    skipped_foreign_keys = _compute_skipped_foreign_keys(tables, registered_names, cycle_members)
    skipped_names_by_table = _index_skipped_names_by_table(skipped_foreign_keys)

    return ForeignKeySyncPlan(
        ordered_tables=tuple(all_tables_in_order),
        skipped_foreign_keys=skipped_foreign_keys,
        skipped_names_by_table=skipped_names_by_table,
    )


def _build_dependency_graph(
    tables: tuple[DesiredTable, ...],
    registered_names: set[str],
) -> dict[str, set[str]]:
    """
    Build an adjacency map from table name to the set of table names it depends on.

    Only in-registry references are included; FK references to tables outside
    the registry are omitted here and classified as UNRESOLVABLE_REFERENCE later.
    """
    graph: dict[str, set[str]] = {str(table.qualified_name): set() for table in tables}
    for table in tables:
        for fk in table.foreign_keys:
            if fk.references in registered_names:
                graph[str(table.qualified_name)].add(fk.references)
    return graph


def _toposort(
    tables: tuple[DesiredTable, ...],
    graph: dict[str, set[str]],
) -> tuple[list[DesiredTable], set[str]]:
    """
    Kahn's topological sort.

    Returns (ordered_tables, cycle_member_names). Tables not involved in a
    cycle appear in dependency-first order. cycle_member_names contains the
    qualified name strings of tables that are part of a cycle (they never
    reached in-degree 0 before the queue was exhausted).
    """
    table_by_name = {str(table.qualified_name): table for table in tables}
    in_degree: dict[str, int] = {name: 0 for name in table_by_name}

    dependents: dict[str, set[str]] = {name: set() for name in table_by_name}
    for name, dependencies in graph.items():
        for dependency in dependencies:
            if dependency in dependents:
                dependents[dependency].add(name)
                in_degree[name] += 1

    queue: deque[str] = deque(name for name, degree in in_degree.items() if degree == 0)
    ordered: list[DesiredTable] = []

    while queue:
        name = queue.popleft()
        ordered.append(table_by_name[name])
        for dependent in dependents[name]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    cycle_members = {name for name, degree in in_degree.items() if degree > 0}
    return ordered, cycle_members


def _compute_skipped_foreign_keys(
    tables: tuple[DesiredTable, ...],
    registered_names: set[str],
    cycle_members: set[str],
) -> tuple[SkippedForeignKey, ...]:
    """
    Classify every FK constraint as skipped or applicable.

    A FK is skipped when its referenced table is not in the registry
    (UNRESOLVABLE_REFERENCE) or its owning table is part of a cycle (CYCLE).
    """
    skipped: list[SkippedForeignKey] = []
    for table in tables:
        table_name = str(table.qualified_name)
        for fk in table.foreign_keys:
            if fk.references not in registered_names:
                skipped.append(
                    SkippedForeignKey(
                        table=table.qualified_name,
                        constraint_name=table.resolve_foreign_key_constraint_name(fk),
                        reason=SkipReason.UNRESOLVABLE_REFERENCE,
                    )
                )
            elif table_name in cycle_members:
                skipped.append(
                    SkippedForeignKey(
                        table=table.qualified_name,
                        constraint_name=table.resolve_foreign_key_constraint_name(fk),
                        reason=SkipReason.CYCLE,
                    )
                )
    return tuple(skipped)


def _index_skipped_names_by_table(
    skipped_foreign_keys: tuple[SkippedForeignKey, ...],
) -> dict[str, frozenset[str]]:
    """Build a per-table index of skipped constraint names for O(1) plan filtering."""
    index: dict[str, set[str]] = {}
    for skipped in skipped_foreign_keys:
        key = str(skipped.table)
        if key not in index:
            index[key] = set()
        index[key].add(skipped.constraint_name)
    return {key: frozenset(names) for key, names in index.items()}
