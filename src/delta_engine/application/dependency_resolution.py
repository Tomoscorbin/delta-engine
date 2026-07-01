"""
Foreign key dependency resolution for sync ordering and failure classification.

The public entry point is `resolve`, which takes the registered tables and
returns them as a tuple of :class:`SyncCandidate` objects in dependency-first
order — referenced tables before their dependents.

Each candidate carries the table it represents and any foreign key failures
that prevent the table from being executed (CYCLE, UNRESOLVABLE_REFERENCE, or
BLOCKED_BY_FAILED_DEPENDENCY). A candidate whose `failures` list is empty may
be executed; one with failures must be excluded.

All graph-traversal implementation details (adjacency map, Tarjan's
strongly-connected-components algorithm, reverse-reachability propagation) are
hidden behind that interface.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from delta_engine.application.results import Failure, ForeignKeyFailure, ForeignKeyFailureReason
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.table import DesiredTable

# A table dependency graph is small (tens of tables, shallow chains), so the
# recursion depth of the SCC traversal below stays far under Python's limit.


def _primary_key_columns(table: DesiredTable) -> tuple[str, ...]:
    """Return the table's primary key column names (empty when it has none)."""
    return table.primary_key.columns if table.primary_key is not None else ()


@dataclass
class SyncCandidate:
    """
    A table prepared for the sync loop, together with its pre-execution failure verdict.

    Attributes:
        table: The desired table to sync.
        failures: All pre-execution failures — external failures (e.g. validation)
            prepended, FK structural failures appended. Assembled by `resolve()`;
            the engine does not modify this after construction.

    """

    table: DesiredTable
    failures: list[Failure] = field(default_factory=list)

    @property
    def qualified_name(self) -> QualifiedName:
        """The fully qualified name of the table this candidate represents."""
        return self.table.qualified_name

    @property
    def can_execute(self) -> bool:
        """True when the table has no pre-execution failures and may be executed."""
        return not self.failures


def resolve(
    tables: tuple[DesiredTable, ...],
    external_failures: dict[QualifiedName, tuple[Failure, ...]] | None = None,
) -> tuple[SyncCandidate, ...]:
    """
    Resolve foreign key dependencies across all registered tables.

    Builds a dependency graph, runs Tarjan's strongly-connected-components
    algorithm to find the safe sync order and detect true cycles, then
    classifies every table as buildable or failed.

    Args:
        tables: All desired tables from the registry, in registry order.
        external_failures: Failures already known before FK resolution — typically
            validation failures computed by the engine. Tables with non-empty
            external failures are treated as will-not-build when propagating
            BLOCKED_BY_FAILED_DEPENDENCY to their FK dependents. Their failures
            are prepended to the candidate's failure list so all pre-execution
            failures appear together.

    Returns:
        A tuple of :class:`SyncCandidate` objects in dependency-first order.
        Each candidate carries its table and any pre-execution failures (external
        failures first, then FK failures). A candidate with an empty failures
        list is ready to sync; one with failures must be excluded from execution.

    """
    external = external_failures or {}
    already_failed = frozenset(
        str(qualified_name)
        for qualified_name, failures in external.items()
        if failures
    )

    registered_names = {str(table.qualified_name) for table in tables}
    graph = _build_dependency_graph(tables, registered_names)
    components = _strongly_connected_components(graph)

    cycle_members = {
        name for component in components if _is_cycle(component) for name in component
    }
    ordered = _order_tables(tables, components)
    failures_by_table = _classify_failures(
        tables, registered_names, cycle_members, already_failed
    )

    return tuple(
        SyncCandidate(
            table=table,
            failures=(
                list(external.get(table.qualified_name, ()))
                + list(failures_by_table.get(table.qualified_name, ()))
            ),
        )
        for table in ordered
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
        for foreign_key in table.foreign_keys:
            if foreign_key.references in registered_names and foreign_key.references != table_name:
                graph[table_name].add(foreign_key.references)
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
    Candidates that cannot execute (FK failures) appear too — the engine gates
    them out via can_execute.
    """
    table_by_name = {str(table.qualified_name): table for table in tables}
    return [table_by_name[name] for component in components for name in component]


def _classify_failures(
    tables: tuple[DesiredTable, ...],
    registered_names: set[str],
    cycle_members: set[str],
    already_failed: frozenset[str] = frozenset(),
) -> dict[QualifiedName, tuple[Failure, ...]]:
    """
    Classify every table as buildable or failed because of a foreign key.

    Two passes:

    1. Direct failures — a foreign key to an unregistered table
       (UNRESOLVABLE_REFERENCE) or any foreign key on a cycle member (CYCLE).
    2. Propagation — a table that references a table which will not be built
       cannot be built either (its foreign key would target a missing table).
       This repeats to a fixpoint so the block flows along chains of dependents.
       `already_failed` seeds this pass with names that failed for external
       reasons (e.g. validation), so their FK dependents are also blocked.
    """
    failures: dict[QualifiedName, list[ForeignKeyFailure]] = {}

    def record(
        table: DesiredTable, foreign_key: ForeignKeyConstraint, reason: ForeignKeyFailureReason
    ) -> None:
        failures.setdefault(table.qualified_name, []).append(
            ForeignKeyFailure(
                table=table.qualified_name,
                constraint_name=foreign_key.resolve_constraint_name(table.qualified_name.name),
                reason=reason,
            )
        )

    # Primary-key columns of every registered table, keyed by qualified name.
    # A foreign key is only valid if its referenced columns are exactly the
    # referenced table's primary key (Databricks rejects other targets at DDL
    # time). Compared as sets: a primary key's declaration order is not part of
    # its identity, and referenced_columns is aligned to local_columns, not PK order.
    primary_key_by_name = {
        str(table.qualified_name): set(_primary_key_columns(table)) for table in tables
    }

    # Pass 1 — direct failures.
    for table in tables:
        table_name = str(table.qualified_name)
        for foreign_key in table.foreign_keys:
            if foreign_key.references not in registered_names:
                record(table, foreign_key, ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE)
            # Checked before cycle membership so that a structural FK-target problem is
            # reported per-FK even when the table also participates in a cycle.
            elif set(foreign_key.referenced_columns) != primary_key_by_name[foreign_key.references]:
                record(table, foreign_key, ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY)
            elif table_name in cycle_members:
                record(table, foreign_key, ForeignKeyFailureReason.CYCLE)

    # Pass 2 — propagate to dependents until no new table is blocked.
    # Seed with both FK direct failures and any externally supplied failed names.
    failed_names = {str(qualified_name) for qualified_name in failures} | already_failed
    changed = True
    while changed:
        changed = False
        for table in tables:
            table_name = str(table.qualified_name)
            if table_name in failed_names:
                continue
            blocking = [fk for fk in table.foreign_keys if fk.references in failed_names]
            if blocking:
                for foreign_key in blocking:
                    record(table, foreign_key, ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY)
                failed_names.add(table_name)
                changed = True

    return {qualified_name: tuple(items) for qualified_name, items in failures.items()}
