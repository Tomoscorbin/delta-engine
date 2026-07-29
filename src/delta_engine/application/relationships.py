"""
Cross-table relationship resolution: ordering, edges, and structural verdicts.

The public entry point is `resolve`, which takes the registered desired tables
and returns one `TableResolution` per table in dependency-first order. It is
pure declaration analysis: no catalog state is consulted, and no work is
planned — differences are the differ's. Each resolution carries the table's
dependency edges (the declared constraints themselves) and its structural
foreign-key verdicts; blocking is inherited at enactment along those edges by
the engine's gating walk.

For example, given these declared foreign keys (child ──► parent)::

    order_items ──► orders ──► customers      invoices ◄──► ledger
    payments ──► invoices                     refunds ──► archive (not registered)

`resolve` orders every table dependency-first and judges each declaration::

    TableResolution(customers)
    TableResolution(orders,      dependencies=(fk → customers,))
    TableResolution(order_items, dependencies=(fk → orders,))
    TableResolution(invoices,    dependencies=(fk → ledger,),   failures=(CYCLE,))
    TableResolution(ledger,      dependencies=(fk → invoices,), failures=(CYCLE,))
    TableResolution(payments,    dependencies=(fk → invoices,))
    TableResolution(refunds,     failures=(UNRESOLVABLE_REFERENCE,))

Healthy tables execute in that order; whether a table is *blocked* by another's
failure is the engine's gating walk following the retained edges in that same
order.

All graph-traversal implementation details (adjacency map, Tarjan's
strongly-connected-components algorithm) are hidden behind that interface.
"""

from collections.abc import Mapping, Set as AbstractSet
from dataclasses import dataclass

from delta_engine.application.failures import (
    ForeignKeyFailure,
    ForeignKeyFailureReason,
)
from delta_engine.domain.model import (
    DataType,
    DesiredTable,
    ForeignKeyConstraint,
    QualifiedName,
    TableAspect,
)

# TODO: Replace the recursive SCC traversal with an iterative implementation.
# Its call depth follows dependency depth, so a chain near Python's recursion
# limit raises RecursionError even though the public API declares no table-count
# or dependency-depth limit. Preserve deterministic dependency-first ordering.


@dataclass(frozen=True, slots=True)
class TableResolution:
    """
    One table's static relationship facts, in dependency order by tuple position.

    ``dependencies`` are the retained dependency edges — the managed foreign
    keys themselves, declared constraints verbatim. Empty
    ``structural_failures`` means the table is structurally sound.
    """

    qualified_name: QualifiedName
    dependencies: tuple[ForeignKeyConstraint, ...]
    structural_failures: tuple[ForeignKeyFailure, ...]


def resolve(tables: tuple[DesiredTable, ...]) -> tuple[TableResolution, ...]:
    """
    Resolve cross-table relationships for one sync.

    Pure declaration analysis: orders every table dependency-first, judges
    each managed foreign key structurally, and retains each table's
    dependency edges as the declared constraints themselves. No catalog
    state is consulted, and no work is planned — differences are the
    differ's, and blocking is inherited at enactment along these edges.
    """
    registered_names = {table.qualified_name for table in tables}
    dependencies_by_table = _build_dependencies(tables, registered_names)
    components = _strongly_connected_components(_build_dependency_graph(dependencies_by_table))
    cycle_partners = _cycle_partners_by_table(components)
    ordered = _order_tables(tables, components)
    failures_by_table = _classify_structural_failures(tables, registered_names, cycle_partners)

    return tuple(
        TableResolution(
            qualified_name=table.qualified_name,
            dependencies=dependencies_by_table[table.qualified_name],
            structural_failures=failures_by_table.get(table.qualified_name, ()),
        )
        for table in ordered
    )


def _managed_foreign_keys(table: DesiredTable) -> tuple[ForeignKeyConstraint, ...]:
    """Return foreign keys this declaration is responsible for reconciling."""
    if TableAspect.FOREIGN_KEYS not in table.managed_aspects:
        return ()
    return table.foreign_keys


def _foreign_key_failure(
    table: DesiredTable,
    foreign_key: ForeignKeyConstraint,
    reason: ForeignKeyFailureReason,
) -> ForeignKeyFailure:
    """Build the failure value associated with one managed foreign key."""
    return ForeignKeyFailure(
        table=table.qualified_name,
        local_columns=foreign_key.local_columns,
        references=foreign_key.referenced_table,
        reason=reason,
    )


def _foreign_key_types_match(
    foreign_key: ForeignKeyConstraint,
    *,
    local_types: Mapping[str, DataType],
    referenced_types: Mapping[str, DataType],
) -> bool:
    """Return True if every local column's type equals its referenced column's type."""
    return all(
        local_types[local_column] == referenced_types[referenced_column]
        for local_column, referenced_column in zip(
            foreign_key.local_columns, foreign_key.referenced_columns, strict=True
        )
    )


def _build_dependencies(
    tables: tuple[DesiredTable, ...],
    registered_names: AbstractSet[QualifiedName],
) -> dict[QualifiedName, tuple[ForeignKeyConstraint, ...]]:
    """
    Build the dependency edges for every table.

    Only references to tables in this sync are included; FK references to tables
    outside it are omitted here and classified as UNRESOLVABLE_REFERENCE later.
    A self-referential FK (references the owning table) is applicable:
    create the table, then add the constraint. It cannot block its own execution,
    so it is excluded from the dependency edges.
    """
    return {
        table.qualified_name: tuple(
            foreign_key
            for foreign_key in _managed_foreign_keys(table)
            if foreign_key.referenced_table in registered_names
            and foreign_key.referenced_table != table.qualified_name
        )
        for table in tables
    }


def _build_dependency_graph(
    dependencies_by_table: dict[QualifiedName, tuple[ForeignKeyConstraint, ...]],
) -> dict[QualifiedName, set[QualifiedName]]:
    """Project dependency edges into the adjacency map used for ordering."""
    return {
        table_name: {foreign_key.referenced_table for foreign_key in dependencies}
        for table_name, dependencies in dependencies_by_table.items()
    }


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


def _classify_structural_failures(
    tables: tuple[DesiredTable, ...],
    registered_names: AbstractSet[QualifiedName],
    cycle_partners_by_table: dict[QualifiedName, frozenset[QualifiedName]],
) -> dict[QualifiedName, tuple[ForeignKeyFailure, ...]]:
    """
    Classify each table's directly-broken foreign keys.

    A foreign key fails structurally when it references an unregistered table
    (UNRESOLVABLE_REFERENCE), targets columns that are not the registered
    table's primary key (REFERENCED_COLUMNS_NOT_A_KEY), disagrees with the
    registered table's column types (REFERENCED_COLUMN_TYPE_MISMATCH), spells
    the registered table's key differently (REFERENCED_COLUMN_CASE_MISMATCH),
    or points into the owning table's own dependency cycle (CYCLE).
    """
    failures: dict[QualifiedName, list[ForeignKeyFailure]] = {}

    def record(
        table: DesiredTable, foreign_key: ForeignKeyConstraint, reason: ForeignKeyFailureReason
    ) -> None:
        failures.setdefault(table.qualified_name, []).append(
            _foreign_key_failure(table, foreign_key, reason)
        )

    # Primary-key columns of every registered table, keyed by qualified name.
    # A foreign key declared through this engine always references the
    # referenced table's primary key — the API validates the mapping's values
    # against it.
    # (Databricks itself also accepts UNIQUE-constraint targets on DBR 18.2+,
    # which this engine does not model.) Compared as sets: a primary key's
    # declaration order is not part of its identity, and referenced_columns is
    # aligned to local_columns, not PK order.
    primary_key_by_name = {
        table.qualified_name: table.primary_key.signature if table.primary_key else frozenset()
        for table in tables
    }

    # Exact spelling of every registered primary key. The signature map above
    # judges the key case-insensitively; this map lets the case arm state
    # drift between two declarations precisely.
    primary_key_spellings_by_name = {
        table.qualified_name: frozenset(
            str(column)
            for column in (table.primary_key.columns if table.primary_key is not None else ())
        )
        for table in tables
    }

    # Column types of every registered table, keyed by qualified name. A
    # foreign key's types were validated at declaration time against the
    # particular parent *object* it was declared with, but the table the sync
    # will actually build is the declaration *registered* under that qualified
    # name — and Databricks requires each foreign-key column type to equal the
    # referenced column's type. The two declarations can differ, so the types
    # are re-checked here against the registered parent.
    column_types_by_name = {
        table.qualified_name: {column.name: column.data_type for column in table.columns}
        for table in tables
    }

    # Judge each managed foreign key against the registered declarations.
    for table in tables:
        table_name = table.qualified_name
        for foreign_key in _managed_foreign_keys(table):
            referenced_table = foreign_key.referenced_table
            if referenced_table not in registered_names:
                record(table, foreign_key, ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE)
            # Structural FK-target checks run before the cycle test so that a
            # structural problem is reported per-FK even when the table also
            # participates in a cycle. The type check relies on the target
            # being the registered parent's primary key: only then is every
            # referenced column known to exist on the registered parent.
            elif foreign_key.referenced_key_signature != primary_key_by_name[referenced_table]:
                record(table, foreign_key, ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY)
            elif not _foreign_key_types_match(
                foreign_key,
                local_types=column_types_by_name[table_name],
                referenced_types=column_types_by_name[referenced_table],
            ):
                record(table, foreign_key, ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH)
            elif {
                str(column) for column in foreign_key.referenced_columns
            } != primary_key_spellings_by_name[referenced_table]:
                # The NOT_A_KEY arm proved these columns ARE the key
                # case-insensitively, so an exact-set mismatch is precisely a
                # case drift between the declaration the FK was built against
                # and the registered one. ADD CONSTRAINT resolves
                # case-sensitively, and the registered declaration is what
                # validation requires to equal the catalog.
                record(table, foreign_key, ForeignKeyFailureReason.REFERENCED_COLUMN_CASE_MISMATCH)
            elif referenced_table in cycle_partners_by_table.get(table_name, frozenset()):
                record(table, foreign_key, ForeignKeyFailureReason.CYCLE)

    return {qualified_name: tuple(items) for qualified_name, items in failures.items()}
