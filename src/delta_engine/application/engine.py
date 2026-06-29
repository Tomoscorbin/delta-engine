"""
High-level orchestration of planning, validation, and execution.

`Engine.sync` reads current catalog state, computes a plan (schema diff +
deterministic ordering), validates it against rules, executes it via provided
adapters, and aggregates results into a `SyncReport`. If any table fails,
`SyncFailedError` is raised with a formatted summary.

Before the four phases, FK dependencies are resolved: `resolve()` returns
tables in dependency-first order as `SyncCandidate` objects. Each candidate
carries its own FK failures; a blocked candidate is excluded from execution.

The sync runs four phases across all tables in sequence:
  1. Read     — fetch current catalog state for every table
  2. Plan     — compute action plans from desired vs observed state
  3. Validate — run validation rules against each plan
  4. Execute  — run passing plans against the catalog

A table that fails in an early phase is carried forward as a partial result
and skipped in later phases, so all tables are attempted and the report is
always complete.
"""

from __future__ import annotations

from datetime import UTC, datetime
import logging

from delta_engine.application.errors import SyncFailedError
from delta_engine.application.foreign_key_planning import SyncCandidate, resolve
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    CatalogState,
    ExecutionSummary,
    ReadFailed,
    SyncReport,
    TablePresent,
    TableRunReport,
    ValidationResult,
)
from delta_engine.application.validation import validate_plan
from delta_engine.domain.model import QualifiedName
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

        Runs four phases across all tables in sequence:
        read → plan → validate → execute. A table that fails in an early
        phase is skipped in later phases; its partial result is included in
        the final report.

        FK dependencies are resolved before the phases: `resolve()` returns
        candidates in dependency-first order. A blocked candidate (one with FK
        failures) is excluded from execution entirely and reported as
        FOREIGN_KEY_FAILED.

        Returns:
            The aggregate :class:`SyncReport` for the run.

        Raises:
            SyncFailedError: If any table fails to read, validate, or execute.
                The report is available on the exception's ``report`` attribute.

        """
        run_started = _utc_now()
        logger.info("Starting sync for %d table(s)", len(registry))

        candidates = resolve(tuple(registry))

        catalog_states = self._read(candidates)
        plans = self._plan(candidates, catalog_states)
        validations = self._validate(plans)
        plans_to_execute = {
            c.table.qualified_name: plans[c.table.qualified_name]
            for c in candidates
            if not c.blocked and not validations[c.table.qualified_name].failed
        }
        executions = self._execute(plans_to_execute)

        table_reports = tuple(
            TableRunReport(
                qualified_name=c.table.qualified_name,
                read=catalog_states[c.table.qualified_name],
                validation=validations[c.table.qualified_name],
                execution=executions.get(c.table.qualified_name, ExecutionSummary()),
                foreign_key_failures=c.failures,
            )
            for c in candidates
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
        candidates: tuple[SyncCandidate, ...],
    ) -> dict[QualifiedName, CatalogState]:
        """Fetch current catalog state for every table."""
        catalog_states: dict[QualifiedName, CatalogState] = {}
        for candidate in candidates:
            qualified_name = candidate.table.qualified_name
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
        candidates: tuple[SyncCandidate, ...],
        catalog_states: dict[QualifiedName, CatalogState],
    ) -> dict[QualifiedName, ActionPlan]:
        """
        Compute an action plan for each table.

        Tables that failed to read get an empty plan.
        """
        plans: dict[QualifiedName, ActionPlan] = {}
        for candidate in candidates:
            qualified_name = candidate.table.qualified_name
            catalog_state = catalog_states[qualified_name]
            if isinstance(catalog_state, ReadFailed):
                plans[qualified_name] = ActionPlan()
                continue

            observed = catalog_state.table if isinstance(catalog_state, TablePresent) else None
            plan = compute_plan(desired=candidate.table, observed=observed)
            plans[qualified_name] = plan
            logger.info("Planned %d action(s) for %s", len(plan), qualified_name)

        return plans

    def _validate(
        self,
        plans: dict[QualifiedName, ActionPlan],
    ) -> dict[QualifiedName, ValidationResult]:
        """Validate every plan in the given dict."""
        validations: dict[QualifiedName, ValidationResult] = {}
        for qualified_name, plan in plans.items():
            validation = validate_plan(plan)
            validations[qualified_name] = validation
            if validation.failed:
                logger.error(
                    "Validation failed for %s (%d failure(s))",
                    qualified_name,
                    len(validation.failures),
                )
            else:
                logger.info("Validation passed for %s", qualified_name)

        return validations

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
