"""
High-level orchestration of planning, validation, and execution.

`Engine.sync` reads current catalog state, computes a plan (schema diff +
deterministic ordering), validates it against rules, resolves FK dependencies
with full failure context, then executes passing plans. If any table fails,
`SyncFailedError` is raised with a formatted summary.

The sync runs five phases across all tables in sequence:
  1. Read     — fetch current catalog state for every table
  2. Plan     — compute action plans from desired vs observed state
  3. Validate — check every plan against rules; collect per-table failures
  4. Resolve  — order tables by FK dependency; propagate all failures
                (FK-structural and validation) to dependents
  5. Execute  — run plans for candidates with no pre-execution failures

Running `resolve()` after validation means a table that fails validation
blocks its FK dependents with BLOCKED_BY_FAILED_DEPENDENCY, not just tables
with FK-structural failures (CYCLE / UNRESOLVABLE_REFERENCE). The rule is
uniform: if a dependency won't reach desired state this sync, its dependents
don't execute either.

A table that fails in an early phase is carried forward as a partial result
and skipped in later phases, so all tables are attempted and the report is
always complete.
"""

from __future__ import annotations

from datetime import UTC, datetime
import logging

from delta_engine.application.errors import SyncFailedError
from delta_engine.application.foreign_key_planning import resolve
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    CatalogState,
    ExecutionSummary,
    Failure,
    ReadFailed,
    SyncReport,
    TablePresent,
    TableRunReport,
)
from delta_engine.application.validation import validate_plan
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import ActionPlan
from delta_engine.domain.plan.differ import compute_plan

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(UTC)


class Engine:
    """
    High-level orchestrator to plan, validate, and execute changes.

    The engine coordinates reading current state from a catalog, computing a
    plan to reach desired state, validating that plan, resolving FK dependencies
    with full failure context, and executing passing plans using the provided
    adapter implementations.
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

        Runs five phases across all tables in sequence:
        read → plan → validate → resolve → execute. Resolve runs after
        validate so that validation failures propagate to FK dependents the
        same way FK-structural failures do.

        A table that fails in an early phase is skipped in later phases;
        its partial result is included in the final report.

        Returns:
            The aggregate :class:`SyncReport` for the run.

        Raises:
            SyncFailedError: If any table fails to read, validate, or execute.
                The report is available on the exception's ``report`` attribute.

        """
        run_started = _utc_now()
        logger.info("Starting sync for %d table(s)", len(registry))

        tables = tuple(registry)
        catalog_states = self._read(tables)
        plans = self._plan(tables, catalog_states)
        validation_failures = self._validate_plans(plans)

        # Merge read failures into external_failures so read-failed tables are
        # treated as "won't build" during FK propagation, blocking their dependents
        # with BLOCKED_BY_FAILED_DEPENDENCY just as validation-failed tables do.
        external_failures: dict[QualifiedName, tuple[Failure, ...]] = dict(validation_failures)
        for table in tables:
            qn = table.qualified_name
            state = catalog_states[qn]
            if isinstance(state, ReadFailed) and not external_failures.get(qn):
                external_failures[qn] = (state.failure,)

        candidates = resolve(tables, external_failures=external_failures)

        plans_to_execute = {
            candidate.table.qualified_name: plans[candidate.table.qualified_name]
            for candidate in candidates
            if candidate.can_execute
        }
        executions = self._execute(plans_to_execute)

        table_reports = tuple(
            TableRunReport(
                qualified_name=candidate.table.qualified_name,
                read=catalog_states[candidate.table.qualified_name],
                pre_execution_failures=tuple(candidate.failures),
                execution=executions.get(candidate.table.qualified_name, ExecutionSummary()),
            )
            for candidate in candidates
        )

        report = SyncReport(
            started_at=run_started,
            ended_at=_utc_now(),
            table_reports=table_reports,
        )

        if report.any_failures:
            raise SyncFailedError(report)

        logger.info("Sync completed successfully for %d table(s)", len(report.table_reports))
        return report

    def _read(
        self,
        tables: tuple[DesiredTable, ...],
    ) -> dict[QualifiedName, CatalogState]:
        """Fetch current catalog state for every table."""
        catalog_states: dict[QualifiedName, CatalogState] = {}
        for table in tables:
            qualified_name = table.qualified_name
            catalog_state = self.reader.fetch_state(qualified_name)
            catalog_states[qualified_name] = catalog_state
            if isinstance(catalog_state, ReadFailed):
                logger.error(
                    "Read failed for %s: %s - %s",
                    qualified_name,
                    catalog_state.failure.exception_type,
                    catalog_state.failure.message,
                )
            else:
                logger.info(
                    "Read state for %s: %s",
                    qualified_name,
                    "present" if isinstance(catalog_state, TablePresent) else "absent",
                )
        return catalog_states

    def _plan(
        self,
        tables: tuple[DesiredTable, ...],
        catalog_states: dict[QualifiedName, CatalogState],
    ) -> dict[QualifiedName, ActionPlan]:
        """
        Compute an action plan for each table.

        Tables that failed to read get an empty plan.
        """
        plans: dict[QualifiedName, ActionPlan] = {}
        for table in tables:
            qualified_name = table.qualified_name
            catalog_state = catalog_states[qualified_name]
            if isinstance(catalog_state, ReadFailed):
                plans[qualified_name] = ActionPlan()
                continue

            observed = catalog_state.table if isinstance(catalog_state, TablePresent) else None
            plan = compute_plan(desired=table, observed=observed)
            plans[qualified_name] = plan
            logger.info("Planned %d action(s) for %s", len(plan), qualified_name)

        return plans

    def _validate_plans(
        self,
        plans: dict[QualifiedName, ActionPlan],
    ) -> dict[QualifiedName, tuple[Failure, ...]]:
        """
        Validate every plan and return per-table failures.

        All tables are validated. The returned dict is passed to `resolve()` so
        that validation-failed tables block their FK dependents.
        """
        validation_failures: dict[QualifiedName, tuple[Failure, ...]] = {}
        for qualified_name, plan in plans.items():
            result = validate_plan(plan)
            validation_failures[qualified_name] = tuple(result.failures)
            if result.failed:
                logger.error(
                    "Validation failed for %s (%d failure(s))",
                    qualified_name,
                    len(result.failures),
                )
            else:
                logger.info("Validation passed for %s", qualified_name)
        return validation_failures

    def _execute(
        self,
        plans: dict[QualifiedName, ActionPlan],
    ) -> dict[QualifiedName, ExecutionSummary]:
        """Execute every plan in the given dict."""
        executions: dict[QualifiedName, ExecutionSummary] = {}
        for qualified_name, plan in plans.items():
            if not plan:
                continue
            execution = self.executor.execute(qualified_name, plan)
            executions[qualified_name] = execution
            logger.info(
                "Executed %d action(s) for %s (%d failed)",
                len(execution.results),
                qualified_name,
                execution.failed_count,
            )
        return executions
