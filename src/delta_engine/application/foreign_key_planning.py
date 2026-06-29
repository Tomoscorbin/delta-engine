"""
Foreign key dependency resolution for sync ordering and failure classification.

The public entry point is `resolve`, which takes the registered tables and
returns a `ForeignKeyResolution` containing:

- The tables in dependency-first order (referenced tables before their
  dependents).
- A per-table verdict: which tables cannot be built because of a foreign key
  problem, and why.

A table fails when a foreign key references a table outside the registry
(UNRESOLVABLE_REFERENCE), when it is part of a true dependency cycle (CYCLE), or
when it (transitively) references a table that itself failed
(BLOCKED_BY_FAILED_DEPENDENCY). A failed table is not built at all — the engine
excludes it from execution and reports the failure.

All graph-traversal implementation details (adjacency map, Tarjan's
strongly-connected-components algorithm, reverse-reachability propagation) are
hidden behind that interface.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from delta_engine.application.results import ForeignKeyFailure, ForeignKeyFailureReason
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.table import DesiredTable

# A table dependency graph is small (tens of tables, shallow chains), so the
# recursion depth of the SCC traversal below stays far under Python's limit.


@dataclass(frozen=True)
class ForeignKeyResolution:
    """
    The result of resolving cross-table foreign key dependencies.

    Attributes:
        ordered_tables: All tables in dependency-first sync order. Referenced
            tables precede their dependents; cycle members appear positioned by
            where their component falls in the order.
        failures_by_table: Per-table foreign key failures, keyed by qualified
            name. A table absent from this mapping has no foreign key problem.

    """

    ordered_tables: tuple[DesiredTable, ...]
    failures_by_table: Mapping[QualifiedName, tuple[ForeignKeyFailure, ...]]

    def fails(self, qualified_name: QualifiedName) -> bool:
        """Return True when this table cannot be executed because of a foreign key problem."""
        return qualified_name in self.failures_by_table

    def failures_for(self, qualified_name: QualifiedName) -> tuple[ForeignKeyFailure, ...]:
        """Return the foreign key failures recorded for this table (empty when none)."""
        return self.failures_by_table.get(qualified_name, ())


def resolve(tables: tuple[DesiredTable, ...]) -> ForeignKeyResolution:
    """
    Resolve foreign key dependencies across all registered tables.

    Builds a dependency graph, runs Tarjan's strongly-connected-components
    algorithm to find the safe sync order and detect true cycles, then
    classifies every table as buildable or failed.

    Args:
        tables: All desired tables from the registry, in registry order.

    Returns:
        A :class:`ForeignKeyResolution` with ordered tables and the per-table
        failure verdict.

    """
    registered_names = {str(table.qualified_name) for table in tables}
    graph = _build_dependency_graph(tables, registered_names)
    components = _strongly_connected_components(graph)

    cycle_members = {
        name for component in components if _is_cycle(component) for name in component
    }
    ordered = _order_tables(tables, components)
    failures_by_table = _classify_failures(tables, registered_names, cycle_members)

    return ForeignKeyResolution(
        ordered_tables=tuple(ordered),
        failures_by_table=failures_by_table,
    )


def _build_dependency_graph(
    tables: tuple[DesiredTable, ...],
    registered_names: set[str],
) -> dict[str, set[str]]:
    """
    Build an adjacency map from table name to the set of table names it depends on.

    Only in-registry references are included; FK references to tables outside
    the registry are omitted here and classified as UNRESOLVABLE_REFERENCE later.
    A self-referential FK (references the owning table) is applicable:
    create the table, then add the constraint. Excluding the self-edge
    keeps the table a non-cyclic single-node component.
    """
    graph: dict[str, set[str]] = {str(table.qualified_name): set() for table in tables}
    for table in tables:
        table_name = str(table.qualified_name)
        for fk in table.foreign_keys:
            if fk.references in registered_names and fk.references != table_name:
                graph[table_name].add(fk.references)
    return graph


def _strongly_connected_components(graph: dict[str, set[str]]) -> list[list[str]]:
    """
    Return the graph's strongly-connected components in dependency-first order.

    Uses Tarjan's algorithm, which emits each component only after every
    component it depends on has been emitted — so a referenced table's component
    always precedes its dependents'. Neighbours are visited in sorted order and
    nodes in graph (registry) insertion order, making the result deterministic
    regardless of set-iteration order or hash seed.

    A component of more than one node is a true dependency cycle. (Self-loops are
    excluded from the graph, so a single node is never cyclic.)
    """
    index_counter = 0
    indices: dict[str, int] = {}
    low_links: dict[str, int] = {}
    on_stack: dict[str, bool] = {}
    stack: list[str] = []
    components: list[list[str]] = []

    def strong_connect(node: str) -> None:
        nonlocal index_counter
        indices[node] = index_counter
        low_links[node] = index_counter
        index_counter += 1
        stack.append(node)
        on_stack[node] = True

        for neighbour in sorted(graph[node]):
            if neighbour not in indices:
                strong_connect(neighbour)
                low_links[node] = min(low_links[node], low_links[neighbour])
            elif on_stack.get(neighbour):
                low_links[node] = min(low_links[node], indices[neighbour])

        if low_links[node] == indices[node]:
            component: list[str] = []
            while True:
                member = stack.pop()
                on_stack[member] = False
                component.append(member)
                if member == node:
                    break
            components.append(component)

    for node in graph:
        if node not in indices:
            strong_connect(node)

    return components


def _is_cycle(component: list[str]) -> bool:
    """Return True if the component is a true multi-node dependency cycle."""
    return len(component) > 1


def _order_tables(
    tables: tuple[DesiredTable, ...],
    components: list[list[str]],
) -> list[DesiredTable]:
    """
    Flatten the SCC components into tables in dependency-first sync order.

    Tarjan emits components dependency-first, so concatenating their members
    yields an order in which every referenced table precedes its dependents.
    Failed tables (cycles, unresolvable, blocked) appear too — the engine gates
    them out of execution by consulting the resolution's failure verdict.
    """
    table_by_name = {str(table.qualified_name): table for table in tables}
    return [table_by_name[name] for component in components for name in component]


def _classify_failures(
    tables: tuple[DesiredTable, ...],
    registered_names: set[str],
    cycle_members: set[str],
) -> dict[QualifiedName, tuple[ForeignKeyFailure, ...]]:
    """
    Classify every table as buildable or failed because of a foreign key.

    Two passes:

    1. Direct failures — a foreign key to an unregistered table
       (UNRESOLVABLE_REFERENCE) or any foreign key on a cycle member (CYCLE).
    2. Propagation — a table that references a table which will not be built
       cannot be built either (its foreign key would target a missing table).
       This repeats to a fixpoint so the block flows along chains of dependents.
    """
    failures: dict[QualifiedName, list[ForeignKeyFailure]] = {}

    def record(
        table: DesiredTable, fk: ForeignKeyConstraint, reason: ForeignKeyFailureReason
    ) -> None:
        failures.setdefault(table.qualified_name, []).append(
            ForeignKeyFailure(
                table=table.qualified_name,
                constraint_name=table.resolve_foreign_key_constraint_name(fk),
                reason=reason,
            )
        )

    # Pass 1 — direct failures.
    for table in tables:
        table_name = str(table.qualified_name)
        for fk in table.foreign_keys:
            if fk.references not in registered_names:
                record(table, fk, ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE)
            elif table_name in cycle_members:
                record(table, fk, ForeignKeyFailureReason.CYCLE)

    # Pass 2 — propagate to dependents until no new table is blocked.
    failed_names = {str(qualified_name) for qualified_name in failures}
    changed = True
    while changed:
        changed = False
        for table in tables:
            table_name = str(table.qualified_name)
            if table_name in failed_names:
                continue
            blocking = [fk for fk in table.foreign_keys if fk.references in failed_names]
            if blocking:
                for fk in blocking:
                    record(table, fk, ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY)
                failed_names.add(table_name)
                changed = True

    return {qualified_name: tuple(items) for qualified_name, items in failures.items()}
