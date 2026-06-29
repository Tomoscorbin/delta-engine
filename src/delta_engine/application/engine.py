"""
High-level orchestration of planning, validation, and execution.

`Engine.sync` reads current catalog state, computes a plan (schema diff +
deterministic ordering), validates it against rules, executes it via provided
adapters, and aggregates results into a `SyncReport`. If any table fails,
`SyncFailedError` is raised with a formatted summary.
"""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime
import logging

from delta_engine.application.errors import (
    SyncFailedError,
)
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    ExecutionSummary,
    ForeignKeyValidationReport,
    ReadFailed,
    SkipReason,
    SkippedForeignKey,
    SyncReport,
    TablePresent,
    TableRunReport,
    ValidationResult,
)
from delta_engine.application.validation import validate_plan
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import ActionPlan, DropForeignKey, SetForeignKey
from delta_engine.domain.plan.differ import compute_plan

logger = logging.getLogger(__name__)


def _build_fk_graph(tables: tuple[DesiredTable, ...]) -> dict[str, set[str]]:
    """
    Build a dependency graph as an adjacency list.

    Maps each table's qualified name string to the set of table names it
    depends on (i.e. the tables its FKs reference that are also in the registry).
    Tables with no in-registry FK dependencies are included with an empty set.
    """
    registered = {str(t.qualified_name) for t in tables}
    graph: dict[str, set[str]] = {str(t.qualified_name): set() for t in tables}
    for table in tables:
        for fk in table.foreign_keys:
            if fk.references in registered:
                graph[str(table.qualified_name)].add(fk.references)
    return graph


def _toposort_tables(
    tables: tuple[DesiredTable, ...],
    graph: dict[str, set[str]],
) -> tuple[list[DesiredTable], set[str]]:
    """
    Kahn's toposort.

    Returns (ordered_tables, cycle_member_names). Tables not in a cycle appear in
    dependency-first order. cycle_member_names contains the qualified name strings
    of any tables that are part of a cycle.
    """
    table_by_name = {str(t.qualified_name): t for t in tables}
    in_degree: dict[str, int] = {name: 0 for name in table_by_name}

    # Build reverse edges: dependents[R] = {tables that depend on R}
    dependents: dict[str, set[str]] = {name: set() for name in table_by_name}
    for name, deps in graph.items():
        for dep in deps:
            if dep in dependents:
                dependents[dep].add(name)
                in_degree[name] += 1

    queue: deque[str] = deque(name for name, deg in in_degree.items() if deg == 0)
    ordered: list[DesiredTable] = []

    while queue:
        name = queue.popleft()
        ordered.append(table_by_name[name])
        for dependent in dependents[name]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    cycle_members = {name for name, deg in in_degree.items() if deg > 0}
    return ordered, cycle_members


def _compute_skipped_fks(
    tables: tuple[DesiredTable, ...],
    graph: dict[str, set[str]],
    cycle_members: set[str],
) -> tuple[SkippedForeignKey, ...]:
    """
    Return the FK constraints that must be skipped before execution.

    A FK is skipped when:
    - Its references value is not in the registry (UNRESOLVABLE_REFERENCE)
    - Its owning table is in a cycle (CYCLE)
    """
    registered = {str(t.qualified_name) for t in tables}
    skipped: list[SkippedForeignKey] = []

    for table in tables:
        table_name = str(table.qualified_name)
        for fk in table.foreign_keys:
            if fk.references not in registered:
                skipped.append(
                    SkippedForeignKey(
                        table=table.qualified_name,
                        constraint_name=table.foreign_key_constraint_name(fk),
                        reason=SkipReason.UNRESOLVABLE_REFERENCE,
                    )
                )
            elif table_name in cycle_members:
                skipped.append(
                    SkippedForeignKey(
                        table=table.qualified_name,
                        constraint_name=table.foreign_key_constraint_name(fk),
                        reason=SkipReason.CYCLE,
                    )
                )
    return tuple(skipped)


def _strip_fk_actions(
    plan: ActionPlan, constraint_names_to_skip: frozenset[str]
) -> ActionPlan:
    """Remove SetForeignKey and DropForeignKey actions for the given constraint names."""
    if not constraint_names_to_skip:
        return plan
    filtered = tuple(
        action
        for action in plan
        if not (
            isinstance(action, (SetForeignKey, DropForeignKey))
            and action.constraint_name in constraint_names_to_skip
        )
    )
    return ActionPlan(filtered)


def _utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(UTC)


class Engine:
    """
    High-level orchestrator to plan, validate, and execute changes.

    The engine coordinates reading current state from a catalog, computing a
    plan to reach desired state, validating that plan, and executing it using
    the provided adapter implementations.
    """

    def __init__(
        self,
        reader: CatalogStateReader,
        executor: PlanExecutor,
    ) -> None:
        """Initialize the engine with the catalog adapters it orchestrates."""
        self.reader = reader
        self.executor = executor

    def sync(self, registry: Registry) -> SyncReport:
        """
        Synchronize all registered tables to their desired state.

        Computes, validates, and executes plans for each table in the supplied
        registry. Before processing individual tables, a cross-table FK graph is
        built to determine sync order (dependency-first via Kahn's toposort) and
        to identify FK constraints that cannot be applied (unresolvable references
        or circular dependencies). Those FK actions are stripped from each table's
        plan before execution; the cycle/skip details are recorded in the report.

        Returns:
            The aggregate :class:`SyncReport` for the run.

        Raises:
            SyncFailedError: If any table fails to read, validate, or execute.
                The report is available on the exception's ``report`` attribute.

        """
        run_started = _utc_now()
        logger.info("Starting sync for %d table(s)", len(registry))

        tables = tuple(registry)
        graph = _build_fk_graph(tables)
        ordered_tables, cycle_members = _toposort_tables(tables, graph)
        # Cycle tables are still synced (their columns/properties are valid); only
        # their FK actions are stripped. Append cycle members after the ordered tables.
        all_tables_in_order = ordered_tables + [
            t for t in tables if str(t.qualified_name) in cycle_members
        ]
        skipped_fks = _compute_skipped_fks(tables, graph, cycle_members)
        fk_validation = ForeignKeyValidationReport(skipped=skipped_fks)

        skipped_names_by_table: dict[str, frozenset[str]] = {}
        for skipped in skipped_fks:
            key = str(skipped.table)
            existing = skipped_names_by_table.get(key, frozenset())
            skipped_names_by_table[key] = existing | {skipped.constraint_name}

        table_reports = [
            self._sync_table(
                t, skipped_names_by_table.get(str(t.qualified_name), frozenset())
            )
            for t in all_tables_in_order
        ]

        report = SyncReport(
            started_at=run_started,
            ended_at=_utc_now(),
            table_reports=tuple(table_reports),
            foreign_key_validation=fk_validation,
        )

        if report.any_failures:
            raise SyncFailedError(report)

        logger.info("Sync completed successfully for %d table(s)", len(report.table_reports))
        return report

    def _sync_table(
        self,
        desired: DesiredTable,
        skipped_fk_constraint_names: frozenset[str] = frozenset(),
    ) -> TableRunReport:
        """
        Synchronize a single table to its desired state.

        Runs the read -> plan -> validate -> execute pipeline, short-circuiting
        when a phase fails. Later phases leave their results at the empty
        default, so the report is assembled once from whatever each phase
        produced.
        """
        started = _utc_now()
        qualified_name = desired.qualified_name
        logger.info("Processing table %s", qualified_name)

        validation = ValidationResult()
        execution = ExecutionSummary()

        catalog_state = self.reader.fetch_state(qualified_name)
        if isinstance(catalog_state, ReadFailed):
            logger.error(
                "Read failed for %s: %s - %s",
                qualified_name,
                catalog_state.failure.exception_type,
                catalog_state.failure.message,
            )
        else:
            observed = catalog_state.table if isinstance(catalog_state, TablePresent) else None
            logger.info(
                "Read state for %s: %s",
                qualified_name,
                "present" if observed is not None else "absent",
            )
            plan = compute_plan(desired=desired, observed=observed)
            plan = _strip_fk_actions(plan, skipped_fk_constraint_names)
            logger.info("Planned %d action(s) for %s", len(plan), qualified_name)
            validation = validate_plan(plan)
            if validation.failed:
                logger.error(
                    "Validation failed for %s (%d failure(s))",
                    qualified_name,
                    len(validation.failures),
                )
            else:
                logger.info("Validation passed for %s", qualified_name)
                execution = self.executor.execute(qualified_name, plan)
                logger.info(
                    "Executed %d action(s) for %s (%d failed)",
                    len(execution.results),
                    qualified_name,
                    execution.failed_count,
                )

        return TableRunReport(
            qualified_name=qualified_name,
            started_at=started,
            ended_at=_utc_now(),
            read=catalog_state,
            validation=validation,
            execution=execution,
        )
