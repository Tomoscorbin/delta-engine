"""
Foreign key dependency resolution for sync ordering and failure classification.

The public entry points are `resolve`, which takes the registered tables and
returns a :class:`ResolveResult` with all table names in dependency-first order
and a mapping of FK failures per table, and `blocking_failures`, which
classifies which of one table's foreign keys reference already-failed tables.
`resolve` uses `blocking_failures` for pre-execution propagation and the
engine reuses it while executing, so both phases block dependents by the
same rule.

For example, given these declared foreign keys (child ──► parent)::

    order_items ──► orders ──► customers      invoices ◄──► ledger
    payments ──► invoices                     refunds ──► archive (not registered)

`resolve` orders every table dependency-first and classifies the failures::

    ordered_names: customers, orders, order_items, ...  (parents before
                   children; failed tables keep their place in the order)
    fk_failures:
        invoices  CYCLE                         (references ledger)
        ledger    CYCLE                         (references invoices)
        payments  BLOCKED_BY_FAILED_DEPENDENCY  (references invoices)
        refunds   UNRESOLVABLE_REFERENCE        (references archive)

Healthy tables execute in that order; the engine gates failed tables out by
their recorded failures.

All graph-traversal implementation details (adjacency map, Tarjan's
strongly-connected-components algorithm, fixpoint propagation of blocked
dependents) are hidden behind that interface.
"""

from collections.abc import Mapping, Set as AbstractSet
from dataclasses import dataclass

from delta_engine.application.failures import ForeignKeyFailure, ForeignKeyFailureReason
from delta_engine.domain.model import DesiredTable, ForeignKeyConstraint, QualifiedName, TableAspect

# A table dependency graph is small (tens of tables, shallow chains), so the
# recursion depth of the SCC traversal below stays far under Python's limit.


@dataclass(frozen=True, slots=True)
class ResolveResult:
    """
    The outcome of foreign-key resolution across all registered tables.

    Attributes:
        ordered_names: All table names in dependency-first order — referenced
            tables before their dependents. Every registered table appears,
            including ones that failed resolution (the engine gates those out
            by their recorded failures).
        fk_failures: The foreign-key failures resolution found, keyed by table.
            A table absent from this mapping had no FK failure.

    """

    ordered_names: tuple[QualifiedName, ...]
    fk_failures: Mapping[QualifiedName, tuple[ForeignKeyFailure, ...]]


def resolve(
    tables: tuple[DesiredTable, ...],
    *,
    blocked: AbstractSet[QualifiedName] = frozenset(),
) -> ResolveResult:
    """
    Resolve foreign key dependencies across all registered tables.

    Builds a dependency graph, runs Tarjan's strongly-connected-components
    algorithm to find the safe sync order and detect true cycles, then
    classifies every table's foreign-key failures.

    Args:
        tables: All desired tables to sync, in prepared (name-sorted) order.
        blocked: Tables already known to be failing before FK resolution
            (e.g. failed read or validation). Their FK dependents are blocked
            with BLOCKED_BY_FAILED_DEPENDENCY, but resolve records no failure
            for the blocked tables themselves — their failures are owned by the
            phase that produced them.

    Returns:
        A :class:`ResolveResult` with all tables in dependency-first
        ``ordered_names`` and ``fk_failures`` keyed by table (absent means
        no FK failure for that table).

    """
    registered_names = {table.qualified_name for table in tables}
    dependencies_by_table = _build_dependency_graph(tables, registered_names)
    components = _strongly_connected_components(dependencies_by_table)

    cycle_partners = _cycle_partners_by_table(components)
    ordered = _order_tables(tables, components)
    failures_by_table = _classify_failures(tables, registered_names, cycle_partners, blocked)

    ordered_names = tuple(table.qualified_name for table in ordered)
    return ResolveResult(ordered_names=ordered_names, fk_failures=failures_by_table)


def blocking_failures(
    table: DesiredTable,
    failed: AbstractSet[QualifiedName],
) -> tuple[ForeignKeyFailure, ...]:
    """
    Classify which of ``table``'s foreign keys are blocked by failed tables.

    Returns one BLOCKED_BY_FAILED_DEPENDENCY failure per foreign key that
    references a table in ``failed``. This is the single owner of the blocking
    rule: `resolve` applies it when propagating pre-execution failures, and the
    engine applies it again while executing, so a dependency that will not
    reach desired state blocks its dependents identically in every phase.
    """
    return tuple(
        ForeignKeyFailure(
            table=table.qualified_name,
            local_columns=foreign_key.local_columns,
            references=foreign_key.referenced_table,
            reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        )
        for foreign_key in _managed_foreign_keys(table)
        if foreign_key.referenced_table in failed
    )


def _managed_foreign_keys(table: DesiredTable) -> tuple[ForeignKeyConstraint, ...]:
    """Return foreign keys this declaration is responsible for reconciling."""
    if TableAspect.FOREIGN_KEYS not in table.managed_aspects:
        return ()
    return table.foreign_keys


def _build_dependency_graph(
    tables: tuple[DesiredTable, ...],
    registered_names: AbstractSet[QualifiedName],
) -> dict[QualifiedName, set[QualifiedName]]:
    """
    Build an adjacency map from table name to the set of table names it depends on.

    Only references to tables in this sync are included; FK references to tables
    outside it are omitted here and classified as UNRESOLVABLE_REFERENCE later.
    A self-referential FK (references the owning table) is applicable:
    create the table, then add the constraint. Excluding the self-edge
    keeps the table a non-cyclic single-node component.
    """
    dependencies_by_table: dict[QualifiedName, set[QualifiedName]] = {
        table.qualified_name: set() for table in tables
    }
    for table in tables:
        table_name = table.qualified_name
        for foreign_key in _managed_foreign_keys(table):
            referenced_table = foreign_key.referenced_table
            if referenced_table in registered_names and referenced_table != table_name:
                dependencies_by_table[table_name].add(referenced_table)
    return dependencies_by_table


def _strongly_connected_components(
    dependencies_by_table: dict[QualifiedName, set[QualifiedName]],
) -> list[list[QualifiedName]]:
    """
    Return the graph's strongly-connected components in dependency-first order.

    Uses Tarjan's algorithm, which emits each component only after every
    component it depends on has been emitted — so a referenced table's component
    always precedes its dependents'. Dependencies are visited in sorted order
    and nodes in graph insertion order, making the result deterministic
    regardless of set-iteration order or hash seed.

    A component of more than one node is a true dependency cycle. (Self-loops are
    excluded from the graph, so a single node is never cyclic.)

    Reference:
    https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    The implementation matches the reference pseudocode, with one deliberate
    divergence: the sorted dependency visits described above.
    """
    # Tarjan's bookkeeping: `indices` numbers nodes in DFS visit order, and
    # `low_links[node]` tracks the smallest index reachable from the node's
    # DFS subtree without leaving the stack. A node whose low-link stays equal
    # to its own index is the root of a strongly-connected component.
    index_counter = 0
    indices: dict[QualifiedName, int] = {}
    low_links: dict[QualifiedName, int] = {}
    on_stack: set[QualifiedName] = set()
    stack: list[QualifiedName] = []
    components: list[list[QualifiedName]] = []

    def strong_connect(node: QualifiedName) -> None:
        nonlocal index_counter
        indices[node] = index_counter
        low_links[node] = index_counter
        index_counter += 1
        stack.append(node)
        on_stack.add(node)

        for dependency in sorted(dependencies_by_table[node], key=str):
            if dependency not in indices:
                strong_connect(dependency)
                low_links[node] = min(low_links[node], low_links[dependency])
            elif dependency in on_stack:
                # A dependency still on the stack is a back-edge into the
                # component being built; one already popped belongs to a
                # completed component and cannot lower this low-link.
                low_links[node] = min(low_links[node], indices[dependency])

        if low_links[node] == indices[node]:
            # This node roots a component: pop the stack down to it to collect its members.
            component: list[QualifiedName] = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                component.append(member)
                if member == node:
                    break
            components.append(component)

    for node in dependencies_by_table:
        if node not in indices:
            strong_connect(node)

    return components


def _is_cycle(component: list[QualifiedName]) -> bool:
    """Return True if the component is a true multi-node dependency cycle."""
    return len(component) > 1


def _cycle_partners_by_table(
    components: list[list[QualifiedName]],
) -> dict[QualifiedName, frozenset[QualifiedName]]:
    """
    Map each member of a cyclic component to the other members of its component.

    A foreign key is part of a cycle exactly when its referenced table is one
    of the owning table's cycle partners; a cycle member's FK to a table
    outside its component is not itself cyclic.
    """
    partners: dict[QualifiedName, frozenset[QualifiedName]] = {}
    for component in components:
        if _is_cycle(component):
            members = frozenset(component)
            for name in component:
                partners[name] = members - {name}
    return partners


def _order_tables(
    tables: tuple[DesiredTable, ...],
    components: list[list[QualifiedName]],
) -> list[DesiredTable]:
    """
    Flatten the SCC components into tables in dependency-first sync order.

    Tarjan emits components dependency-first, so concatenating their members
    yields an order in which every referenced table precedes its dependents.
    Tables that cannot execute (FK failures) appear too — the engine gates
    them out by their recorded failures.
    """
    table_by_name = {table.qualified_name: table for table in tables}
    return [table_by_name[name] for component in components for name in component]


def _classify_failures(
    tables: tuple[DesiredTable, ...],
    registered_names: AbstractSet[QualifiedName],
    cycle_partners_by_table: dict[QualifiedName, frozenset[QualifiedName]],
    already_failed: AbstractSet[QualifiedName],
) -> dict[QualifiedName, tuple[ForeignKeyFailure, ...]]:
    """
    Classify every table as buildable or failed because of a foreign key.

    Two passes:

    1. Direct failures — a foreign key to an unregistered table
       (UNRESOLVABLE_REFERENCE) or a foreign key into the owning table's own
       dependency cycle (CYCLE).
    2. Propagation — a table that references a table which will not be built
       cannot be built either (its foreign key would target a missing table).
       This repeats to a fixpoint so the block flows along chains of dependents.
       `already_failed` seeds this pass with names that failed for external
       reasons (e.g. validation), so their FK dependents are also blocked.

    For example, with `archive` not registered::

        c ──► b ──► a ──► archive

    pass 1 fails `a` (UNRESOLVABLE_REFERENCE) and pass 2 blocks `b` and `c`
    (BLOCKED_BY_FAILED_DEPENDENCY).
    """
    failures: dict[QualifiedName, list[ForeignKeyFailure]] = {}

    def record(
        table: DesiredTable, foreign_key: ForeignKeyConstraint, reason: ForeignKeyFailureReason
    ) -> None:
        failures.setdefault(table.qualified_name, []).append(
            ForeignKeyFailure(
                table=table.qualified_name,
                local_columns=foreign_key.local_columns,
                references=foreign_key.referenced_table,
                reason=reason,
            )
        )

    # Primary-key columns of every registered table, keyed by qualified name.
    # A foreign key declared through this engine always references the
    # referenced table's primary key — the API validates the mapping's values
    # against it.
    # (Databricks itself also accepts UNIQUE-constraint targets on DBR 18.2+,
    # which this engine does not model.) Compared as sets: a primary key's
    # declaration order is not part of its identity, and referenced_columns is
    # aligned to local_columns, not PK order.
    primary_key_by_name = {table.qualified_name: set(table.primary_key_columns) for table in tables}

    # Pass 1 — direct failures.
    for table in tables:
        table_name = table.qualified_name
        for foreign_key in _managed_foreign_keys(table):
            referenced_table = foreign_key.referenced_table
            if referenced_table not in registered_names:
                record(table, foreign_key, ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE)
            # Checked before the cycle test so that a structural FK-target problem is
            # reported per-FK even when the table also participates in a cycle.
            elif set(foreign_key.referenced_columns) != primary_key_by_name[referenced_table]:
                record(table, foreign_key, ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY)
            elif referenced_table in cycle_partners_by_table.get(table_name, frozenset()):
                record(table, foreign_key, ForeignKeyFailureReason.CYCLE)

    # Pass 2 — propagate to dependents until no new table is blocked.
    # Seed with both FK direct failures and any externally supplied failed names.
    # Materialize a builtin set: the fixpoint loop below mutates it, and
    # already_failed may be any read-only AbstractSet.
    failed_names = set(failures) | set(already_failed)
    changed = True
    while changed:
        changed = False
        for table in tables:
            table_name = table.qualified_name
            if table_name in failed_names:
                continue
            blocking = blocking_failures(table, failed_names)
            if blocking:
                failures.setdefault(table_name, []).extend(blocking)
                failed_names.add(table_name)
                changed = True

    return {qualified_name: tuple(items) for qualified_name, items in failures.items()}
