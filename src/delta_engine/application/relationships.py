"""
Cross-table relationship resolution: ordering, classification, and FK planning.

The public entry point is `resolve`, which takes each registered table with its
read snapshot and returns one explicit success or failure per table in
dependency-first order. A successful resolution carries the planned set/drop
foreign-key actions (spelled as the catalogs spell them) and retains its
resolved dependencies so the engine's gating walk can block dependents of
tables that fail, without reinterpreting the desired table declaration.

For example, given these declared foreign keys (child ──► parent)::

    order_items ──► orders ──► customers      invoices ◄──► ledger
    payments ──► invoices                     refunds ──► archive (not registered)

`resolve` orders every table dependency-first and classifies its outcome::

    ResolutionSucceeded(customers)
    ResolutionSucceeded(orders, actions=(SetForeignKey(...),))
    ResolutionSucceeded(order_items, actions=(SetForeignKey(...),))
    ResolutionFailed(invoices, CYCLE)
    ResolutionFailed(ledger, CYCLE)
    ResolutionSucceeded(payments, dependencies=(invoices,))
    ResolutionFailed(refunds, UNRESOLVABLE_REFERENCE)

Healthy tables execute in that order; whether a table is *blocked* by another's
failure is the engine's gating walk applying ``blocking_failures`` in that same
order.

All graph-traversal implementation details (adjacency map, Tarjan's
strongly-connected-components algorithm) are hidden behind that interface.
"""

from collections.abc import Mapping, Set as AbstractSet
from dataclasses import dataclass, replace

from delta_engine.application.failures import (
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
)
from delta_engine.application.ports import ReadResult, TablePresent
from delta_engine.domain.model import (
    DataType,
    DesiredTable,
    ForeignKeyConstraint,
    ObservedTable,
    QualifiedName,
    TableAspect,
)
from delta_engine.domain.plan import DropForeignKey, SetForeignKey

# TODO: Replace the recursive SCC traversal with an iterative implementation.
# Its call depth follows dependency depth, so a chain near Python's recursion
# limit raises RecursionError even though the public API declares no table-count
# or dependency-depth limit. Preserve deterministic dependency-first ordering.


@dataclass(frozen=True, slots=True)
class ResolvedDependency:
    """An execution-ready dependency edge produced by foreign-key resolution."""

    referenced_table: QualifiedName
    blocked_failure: ForeignKeyFailure


@dataclass(frozen=True, slots=True)
class ResolutionSucceeded:
    """A table placed in dependency order with its planned relationship work."""

    qualified_name: QualifiedName
    dependencies: tuple[ResolvedDependency, ...]
    actions: tuple[SetForeignKey | DropForeignKey, ...] = ()

    def blocking_failures(
        self, failed: AbstractSet[QualifiedName]
    ) -> tuple[ForeignKeyFailure, ...]:
        """Return the failures blocking this table, given names that will not converge."""
        return tuple(
            dependency.blocked_failure
            for dependency in self.dependencies
            if dependency.referenced_table in failed
        )


@dataclass(frozen=True, slots=True)
class ResolutionFailed:
    """A table placed in dependency order that cannot sync because of its foreign keys."""

    qualified_name: QualifiedName
    failures: tuple[ForeignKeyFailure, ...]
    actions: tuple[SetForeignKey | DropForeignKey, ...] = ()

    def __post_init__(self) -> None:
        if not self.failures:
            raise ValueError("ResolutionFailed requires at least one foreign-key failure")


type TableResolution = ResolutionSucceeded | ResolutionFailed
type ResolveResult = tuple[TableResolution, ...]
type TableSnapshot = tuple[DesiredTable, ReadResult]


def resolve(tables: tuple[TableSnapshot, ...]) -> tuple[TableResolution, ...]:
    """
    Resolve cross-table relationships for one sync.

    Pure function of the read snapshots: orders every table dependency-first,
    judges each declared foreign key structurally, and plans the set/drop
    actions that converge each table's foreign keys — spelled as the catalogs
    spell them, because this is the one module holding both endpoints.
    Blocking is not computed here: successful resolutions expose
    ``blocking_failures`` and the engine's gating walk applies it.
    """
    desired_tables = tuple(desired for desired, _ in tables)
    registered_names = {table.qualified_name for table in desired_tables}
    observed_by_name = {
        desired.qualified_name: read.table
        for desired, read in tables
        if isinstance(read, TablePresent)
    }
    read_by_name = {desired.qualified_name: read for desired, read in tables}

    resolved_dependencies = _build_resolved_dependencies(desired_tables, registered_names)
    components = _strongly_connected_components(_build_dependency_graph(resolved_dependencies))
    cycle_partners = _cycle_partners_by_table(components)
    ordered = _order_tables(desired_tables, components)
    failures_by_table = _classify_structural_failures(
        desired_tables, registered_names, cycle_partners
    )
    spellings_by_table = {
        desired.qualified_name: _effective_spellings(
            desired, observed_by_name.get(desired.qualified_name)
        )
        for desired in desired_tables
    }

    resolutions: list[TableResolution] = []
    for table in ordered:
        qualified_name = table.qualified_name
        actions = _foreign_key_actions(table, read_by_name[qualified_name], spellings_by_table)
        failures = failures_by_table.get(qualified_name)
        if failures is None:
            resolutions.append(
                ResolutionSucceeded(
                    qualified_name=qualified_name,
                    dependencies=resolved_dependencies[qualified_name],
                    actions=actions,
                )
            )
        else:
            resolutions.append(
                ResolutionFailed(
                    qualified_name=qualified_name, failures=tuple(failures), actions=actions
                )
            )
    return tuple(resolutions)


def resolve_with_blocking(
    tables: tuple[DesiredTable, ...],
    *,
    failed_names: AbstractSet[QualifiedName] = frozenset(),
) -> ResolveResult:
    """
    Resolve foreign key dependencies across all registered tables.

    Builds a dependency graph, runs Tarjan's strongly-connected-components
    algorithm to find the safe sync order and detect true cycles, then
    classifies every table's foreign-key failures.

    Args:
        tables: All desired tables to sync, in prepared (name-sorted) order.
        failed_names: Tables already known to be failing before FK resolution
            (e.g. failed read or planning). Their FK dependents are blocked with
            BLOCKED_BY_FAILED_DEPENDENCY, but resolution succeeds for the failed
            tables themselves unless they have their own foreign-key failure —
            their earlier failures remain owned by the phase that produced them.

    Returns:
        One explicit success or failure per table, in dependency-first order.
        Successful outcomes retain the resolved dependencies needed to react
        to failures that arise later during execution.

    """
    registered_names = {table.qualified_name for table in tables}
    resolved_dependencies_by_table = _build_resolved_dependencies(tables, registered_names)
    dependencies_by_table = _build_dependency_graph(resolved_dependencies_by_table)
    components = _strongly_connected_components(dependencies_by_table)

    cycle_partners = _cycle_partners_by_table(components)
    ordered = _order_tables(tables, components)
    failures_by_table = _classify_failures(
        tables,
        registered_names,
        cycle_partners,
        failed_names,
        resolved_dependencies_by_table,
    )

    resolutions: list[TableResolution] = []
    for table in ordered:
        qualified_name = table.qualified_name
        failures = failures_by_table.get(qualified_name)
        if failures is None:
            resolutions.append(
                ResolutionSucceeded(
                    qualified_name=qualified_name,
                    dependencies=resolved_dependencies_by_table[qualified_name],
                )
            )
        else:
            resolutions.append(
                ResolutionFailed(
                    qualified_name=qualified_name,
                    failures=failures,
                )
            )
    return tuple(resolutions)


def _managed_foreign_keys(table: DesiredTable) -> tuple[ForeignKeyConstraint, ...]:
    """Return foreign keys this declaration is responsible for reconciling."""
    if TableAspect.FOREIGN_KEYS not in table.managed_aspects:
        return ()
    return table.foreign_keys


type _SpellingMap = Mapping[str, str]


def _effective_spellings(desired: DesiredTable, observed: ObservedTable | None) -> _SpellingMap:
    """Effective spelling per column: the catalog's where present, else the declared."""
    declared: dict[str, str] = {column.name: column.name for column in desired.columns}
    if observed is None:
        return declared
    return declared | {column.name: column.name for column in observed.columns}


def _respell_constraint(
    constraint: ForeignKeyConstraint,
    own: _SpellingMap,
    spellings_by_table: Mapping[QualifiedName, _SpellingMap],
) -> ForeignKeyConstraint:
    """ADD CONSTRAINT is case-sensitive on both sides; spell as the catalogs do."""
    referenced: _SpellingMap = spellings_by_table.get(constraint.referenced_table, {})
    return replace(
        constraint,
        local_columns=tuple(own.get(name, name) for name in constraint.local_columns),
        referenced_columns=tuple(
            referenced.get(name, name) for name in constraint.referenced_columns
        ),
    )


def _foreign_key_actions(
    desired: DesiredTable,
    read: ReadResult,
    spellings_by_table: Mapping[QualifiedName, _SpellingMap],
) -> tuple[SetForeignKey | DropForeignKey, ...]:
    """Set/drop actions converging the table's declared foreign keys, catalog-spelled."""
    if isinstance(read, ReadFailure):
        return ()
    observed_constraints = read.table.foreign_keys if isinstance(read, TablePresent) else ()
    desired_by_signature = {fk.signature: fk for fk in desired.foreign_keys}
    observed_by_signature = {fk.signature: fk for fk in observed_constraints}
    own = spellings_by_table[desired.qualified_name]
    actions: list[SetForeignKey | DropForeignKey] = []
    for signature, constraint in desired_by_signature.items():
        if signature not in observed_by_signature:
            actions.append(
                SetForeignKey(constraint=_respell_constraint(constraint, own, spellings_by_table))
            )
    for signature, constraint in observed_by_signature.items():
        if signature not in desired_by_signature:
            actions.append(DropForeignKey(constraint=constraint))
    return tuple(actions)


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


def _build_resolved_dependencies(
    tables: tuple[DesiredTable, ...],
    registered_names: AbstractSet[QualifiedName],
) -> dict[QualifiedName, tuple[ResolvedDependency, ...]]:
    """
    Build execution-ready dependency edges for every table.

    Only references to tables in this sync are included; FK references to tables
    outside it are omitted here and classified as UNRESOLVABLE_REFERENCE later.
    A self-referential FK (references the owning table) is applicable:
    create the table, then add the constraint. It cannot block its own execution,
    so it is excluded from the dependency edges.
    """
    dependencies_by_table: dict[QualifiedName, tuple[ResolvedDependency, ...]] = {}
    for table in tables:
        table_name = table.qualified_name
        dependencies_by_table[table_name] = tuple(
            ResolvedDependency(
                referenced_table=foreign_key.referenced_table,
                blocked_failure=_foreign_key_failure(
                    table,
                    foreign_key,
                    ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
                ),
            )
            for foreign_key in _managed_foreign_keys(table)
            if foreign_key.referenced_table in registered_names
            and foreign_key.referenced_table != table_name
        )
    return dependencies_by_table


def _build_dependency_graph(
    resolved_dependencies_by_table: dict[QualifiedName, tuple[ResolvedDependency, ...]],
) -> dict[QualifiedName, set[QualifiedName]]:
    """Project resolved dependency edges into the adjacency map used for ordering."""
    return {
        table_name: {dependency.referenced_table for dependency in dependencies}
        for table_name, dependencies in resolved_dependencies_by_table.items()
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
    registered table's column types (REFERENCED_COLUMN_TYPE_MISMATCH), or
    points into the owning table's own dependency cycle (CYCLE).
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
            elif referenced_table in cycle_partners_by_table.get(table_name, frozenset()):
                record(table, foreign_key, ForeignKeyFailureReason.CYCLE)

    return {qualified_name: tuple(items) for qualified_name, items in failures.items()}


def _classify_failures(
    tables: tuple[DesiredTable, ...],
    registered_names: AbstractSet[QualifiedName],
    cycle_partners_by_table: dict[QualifiedName, frozenset[QualifiedName]],
    already_failed: AbstractSet[QualifiedName],
    resolved_dependencies_by_table: dict[
        QualifiedName,
        tuple[ResolvedDependency, ...],
    ],
) -> dict[QualifiedName, tuple[ForeignKeyFailure, ...]]:
    """
    Classify every table as buildable or failed because of a foreign key.

    Structural failures come from :func:`_classify_structural_failures`; on top
    of them, propagation — a table that references a table which will not be
    built cannot be built either (its foreign key would target a missing
    table). This repeats to a fixpoint so the block flows along chains of
    dependents. `already_failed` seeds this pass with names that failed for
    external reasons (e.g. planning), so their FK dependents are also blocked.

    For example, with `archive` not registered::

        c ──► b ──► a ──► archive

    the structural pass fails `a` (UNRESOLVABLE_REFERENCE) and propagation
    blocks `b` and `c` (BLOCKED_BY_FAILED_DEPENDENCY).
    """
    failures = {
        qualified_name: list(items)
        for qualified_name, items in _classify_structural_failures(
            tables, registered_names, cycle_partners_by_table
        ).items()
    }

    # Propagate to dependents until no new table is blocked. Seed with both FK
    # direct failures and any externally supplied failed names. Materialize a
    # builtin set: the fixpoint loop below mutates it, and already_failed may
    # be any read-only AbstractSet.
    failed_names = set(failures) | set(already_failed)
    changed = True
    while changed:
        changed = False
        for table in tables:
            table_name = table.qualified_name
            if table_name in failed_names:
                continue
            dependency_failures = tuple(
                dependency.blocked_failure
                for dependency in resolved_dependencies_by_table[table_name]
                if dependency.referenced_table in failed_names
            )
            if dependency_failures:
                failures.setdefault(table_name, []).extend(dependency_failures)
                failed_names.add(table_name)
                changed = True

    return {qualified_name: tuple(items) for qualified_name, items in failures.items()}
