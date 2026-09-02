"""
Cross-table relationship resolution: ordering, edges, and structural verdicts.

The public entry point is `resolve`, which takes the registered desired tables
and returns one `TableResolution` per table in dependency-first order. It is
pure declaration analysis: no catalog state is consulted, and no work is
planned — differences are the differ's. Each resolution carries the
declaration it judged, that table's dependency edges (the declared
constraints themselves), and its structural foreign-key verdicts, and states
through `blocked_by` which of those edges block it once the caller knows which
tables will not converge.

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

Healthy tables execute in that order. Whether a table is *blocked* by another's
failure depends on how the run goes, so the caller supplies the tables that
will not converge and each resolution names its own blocked edges — folded
over the tables in that same order, the block propagates along FK chains.

All graph-traversal implementation details, including the adjacency map and
strongly-connected-components algorithm, are hidden behind that interface.
"""

from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
import logging

from delta_engine.application.failures import (
    ForeignKeyFailure,
    ForeignKeyFailureReason,
)
from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import (
    DataType,
    DesiredTable,
    ForeignKeyConstraint,
    QualifiedName,
    TableAspect,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TableResolution:
    """
    One table's static relationship facts, in dependency order by tuple position.

    ``desired`` is the declaration these facts were judged from, so a
    resolution is self-sufficient: callers read the table from it rather than
    looking it back up by name. ``dependencies`` are the retained dependency
    edges — the managed foreign keys themselves, declared constraints
    verbatim. Empty ``structural_failures`` means the table is structurally
    sound.
    """

    desired: DesiredTable
    dependencies: ListOrTuple[ForeignKeyConstraint]
    structural_failures: ListOrTuple[ForeignKeyFailure]

    def __post_init__(self) -> None:
        object.__setattr__(self, "dependencies", tuple(self.dependencies))
        object.__setattr__(self, "structural_failures", tuple(self.structural_failures))

    @property
    def qualified_name(self) -> QualifiedName:
        """The identity of the declaration these facts were judged from."""
        return self.desired.qualified_name

    def blocked_by(self, unconverged: Set[QualifiedName]) -> tuple[ForeignKeyFailure, ...]:
        """
        Return one failure per dependency edge that will not converge this sync.

        Empty when no edge points into ``unconverged``, which is what a caller
        walking tables in dependency order treats as "free to enact". Which
        tables will not converge is a fact about the run, not about the
        declarations, so it is supplied rather than resolved: the caller
        accumulates it as reads, plans, and statements fail.
        """
        return tuple(
            ForeignKeyFailure(
                table=self.qualified_name,
                local_columns=dependency.local_columns,
                references=dependency.referenced_table,
                reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
            )
            for dependency in self.dependencies
            if dependency.referenced_table in unconverged
        )


def resolve(tables: tuple[DesiredTable, ...]) -> tuple[TableResolution, ...]:
    """
    Resolve cross-table relationships for one sync.

    Pure declaration analysis: orders every table dependency-first, judges
    each managed foreign key structurally, and retains each declaration
    alongside its dependency edges as the declared constraints themselves.
    No catalog state is consulted, and no work is planned — differences are
    the differ's, and blocking is inherited at enactment along these edges
    via :meth:`TableResolution.blocked_by`.
    """
    registered_names = {table.qualified_name for table in tables}
    dependencies_by_table = _build_dependencies(tables, registered_names)
    components = _strongly_connected_components(_build_dependency_graph(dependencies_by_table))
    cycle_partners = _cycle_partners_by_table(components)
    ordered = _order_tables(tables, components)
    failures_by_table = _classify_structural_failures(tables, registered_names, cycle_partners)

    for qualified_name in failures_by_table:
        logger.error("Foreign key resolution failed for %s", qualified_name)

    return tuple(
        TableResolution(
            desired=table,
            dependencies=dependencies_by_table[table.qualified_name],
            structural_failures=failures_by_table.get(table.qualified_name, ()),
        )
        for table in ordered
    )


def _managed_foreign_keys(table: DesiredTable) -> Sequence[ForeignKeyConstraint]:
    """Return foreign keys this declaration is responsible for reconciling."""
    if not table.scope.manages(TableAspect.FOREIGN_KEYS):
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
    registered_names: Set[QualifiedName],
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

    Uses iterative Kosaraju traversal so dependency depth does not consume the
    Python call stack. Dependencies are visited in sorted order and roots in
    graph insertion order, making the result deterministic regardless of set
    iteration order or hash seed.

    A component of more than one node is a true dependency cycle. (Self-loops are
    excluded from the graph, so a single node is never cyclic.)
    """
    visited: set[QualifiedName] = set()
    finishing_order: list[QualifiedName] = []

    # Record DFS finishing order without recursive calls. The boolean stack
    # entry is the iterative equivalent of returning from a DFS call.
    for root in dependencies_by_table:
        if root in visited:
            continue

        pending_visits: list[tuple[QualifiedName, bool]] = [(root, False)]
        while pending_visits:
            node, exiting = pending_visits.pop()
            if exiting:
                finishing_order.append(node)
                continue
            if node in visited:
                continue

            visited.add(node)
            pending_visits.append((node, True))
            pending_visits.extend(
                (dependency, False)
                for dependency in reversed(sorted(dependencies_by_table[node], key=str))
            )

    # Reverse table -> dependency edges into dependency -> dependent edges for
    # Kosaraju's component-collection pass.
    dependents_by_table: dict[QualifiedName, set[QualifiedName]] = {
        name: set() for name in dependencies_by_table
    }
    for table, dependencies in dependencies_by_table.items():
        for dependency in dependencies:
            dependents_by_table[dependency].add(table)

    visited.clear()
    components: list[list[QualifiedName]] = []
    for root in reversed(finishing_order):
        if root in visited:
            continue

        component: list[QualifiedName] = []
        pending_tables = [root]
        visited.add(root)
        while pending_tables:
            node = pending_tables.pop()
            component.append(node)
            for dependent in reversed(sorted(dependents_by_table[node], key=str)):
                if dependent not in visited:
                    visited.add(dependent)
                    pending_tables.append(dependent)

        components.append(component)

    # With table -> dependency edges, the second pass discovers dependents
    # first. Reverse the components so dependencies precede their consumers.
    components.reverse()
    return components


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
        # A single-node component is not a cycle; more than one member is.
        if len(component) > 1:
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

    Components are already dependency-first, so concatenating their members
    yields an order in which every referenced table precedes its dependents.
    Tables that cannot execute (FK failures) appear too — the engine gates
    them out by their recorded failures.
    """
    table_by_name = {table.qualified_name: table for table in tables}
    return [table_by_name[name] for component in components for name in component]


def _classify_structural_failures(
    tables: tuple[DesiredTable, ...],
    registered_names: Set[QualifiedName],
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

    # Primary key of every registered table, keyed by qualified name.
    # A foreign key declared through this engine always references the
    # referenced table's primary key — the API validates the mapping's values
    # against it.
    # (Databricks itself also accepts UNIQUE-constraint targets on DBR 18.2+,
    # which this engine does not model.) The primary-key value owns the
    # order-independent, case-insensitive column matching rule.
    primary_key_by_name = {table.qualified_name: table.primary_key for table in tables}

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
                continue

            primary_key = primary_key_by_name[referenced_table]
            # Structural FK-target checks run before the cycle test so that a
            # structural problem is reported per-FK even when the table also
            # participates in a cycle. The type check relies on the target
            # being the registered parent's primary key: only then is every
            # referenced column known to exist on the registered parent.
            if primary_key is None or not primary_key.matches_columns(
                foreign_key.referenced_columns
            ):
                record(table, foreign_key, ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY)
            elif not _foreign_key_types_match(
                foreign_key,
                local_types=column_types_by_name[table_name],
                referenced_types=column_types_by_name[referenced_table],
            ):
                record(table, foreign_key, ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH)
            elif {str(column) for column in foreign_key.referenced_columns} != {
                str(column) for column in primary_key.columns
            }:
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
