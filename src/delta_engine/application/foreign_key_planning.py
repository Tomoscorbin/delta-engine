"""
Foreign key dependency resolution for sync ordering and constraint skipping.

The public entry point is `resolve`, which takes the registered tables and
returns a `ForeignKeySyncPlan` containing:

- The tables in dependency-first order (referenced tables before their
  dependents), with cycle members appended at the end.
- The FK constraints that cannot be applied, with their skip reason.
- A per-table index of skipped constraint names, ready for plan filtering.

All graph-traversal implementation details (adjacency map, Tarjan's
strongly-connected-components algorithm) are hidden behind that interface.
"""

from __future__ import annotations

from dataclasses import dataclass

from delta_engine.application.results import SkippedForeignKey, SkipReason
from delta_engine.domain.model.table import DesiredTable

# A table dependency graph is small (tens of tables, shallow chains), so the
# recursion depth of the SCC traversal below stays far under Python's limit.


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

    Builds a dependency graph, runs Tarjan's strongly-connected-components
    algorithm to find the safe sync order and detect true cycles, then
    classifies every FK constraint as either applicable or skipped
    (CYCLE / UNRESOLVABLE_REFERENCE). A table that merely depends on a cycle is
    not itself a cycle member — its FK is applied normally.

    Args:
        tables: All desired tables from the registry, in registry order.

    Returns:
        A :class:`ForeignKeySyncPlan` with ordered tables, skipped FKs,
        and a per-table lookup of constraint names to suppress.

    """
    registered_names = {str(table.qualified_name) for table in tables}
    graph = _build_dependency_graph(tables, registered_names)
    components = _strongly_connected_components(graph)

    cycle_members = {
        name for component in components if _is_cycle(component, graph) for name in component
    }
    ordered = _order_tables(tables, components)

    skipped_foreign_keys = _compute_skipped_foreign_keys(tables, registered_names, cycle_members)
    skipped_names_by_table = _index_skipped_names_by_table(skipped_foreign_keys)

    return ForeignKeySyncPlan(
        ordered_tables=tuple(ordered),
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
        table_name = str(table.qualified_name)
        for fk in table.foreign_keys:
            # A self-referential FK (references the owning table) is applicable:
            # create the table, then add the constraint. Excluding the self-edge
            # keeps the table a non-cyclic single-node component.
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

    A component of more than one node is a true dependency cycle; a single node
    that references itself is a self-loop cycle. Any other component is a single
    acyclic table.
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


def _is_cycle(component: list[str], graph: dict[str, set[str]]) -> bool:
    """Return True if the component is a dependency cycle (multi-node or self-loop)."""
    return len(component) > 1 or component[0] in graph[component[0]]


def _order_tables(
    tables: tuple[DesiredTable, ...],
    components: list[list[str]],
) -> list[DesiredTable]:
    """
    Flatten the SCC components into tables in dependency-first sync order.

    Tarjan emits components dependency-first, so concatenating their members
    yields an order in which every referenced table precedes its dependents.
    Cycle members appear too — their non-FK changes still sync once their FK
    actions are stripped — positioned by where their component falls in the
    dependency order.
    """
    table_by_name = {str(table.qualified_name): table for table in tables}
    return [table_by_name[name] for component in components for name in component]


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
