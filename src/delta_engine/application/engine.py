"""
High-level orchestration of planning, validation, and execution.

`Engine.sync` reads current catalog state, computes a plan (schema diff +
deterministic ordering), validates it against rules, executes it via provided
adapters, and aggregates results into a `SyncReport`. If any table fails,
`SyncFailedError` is raised with a formatted summary.
"""

from __future__ import annotations

from datetime import UTC, datetime
import logging

from delta_engine.application.errors import (
    SyncFailedError,
)
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    ExecutionSummary,
    ReadFailed,
    SyncReport,
    TablePresent,
    TableRunReport,
    ValidationResult,
)
from delta_engine.application.validation import validate_plan
from delta_engine.domain.model.table import DesiredTable
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

        Computes, validates, and executes plans for each table in the supplied
        registry.

        Returns:
            The aggregate :class:`SyncReport` for the run.

        Raises:
            SyncFailedError: If any table fails to read, validate, or execute.
                The report is available on the exception's ``report`` attribute.

        """
        run_started = _utc_now()
        logger.info("Starting sync for %d table(s)", len(registry))
        table_reports = [self._sync_table(t) for t in registry]

        report = SyncReport(
            started_at=run_started,
            ended_at=_utc_now(),
            table_reports=tuple(table_reports),
        )

        if report.any_failures:
            raise SyncFailedError(report)

        logger.info("Sync completed successfully for %d table(s)", len(report.table_reports))
        return report

    def _sync_table(self, desired: DesiredTable) -> TableRunReport:
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
