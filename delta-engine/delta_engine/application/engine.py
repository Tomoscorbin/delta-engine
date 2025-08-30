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
from delta_engine.application.format_report import format_sync_report
from delta_engine.application.plan import (
    make_plan_context,
)
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import SyncReport, TableRunReport, ValidationResult
from delta_engine.application.validation import DEFAULT_VALIDATOR, PlanValidator
from delta_engine.domain.model.table import DesiredTable

logger = logging.getLogger(__name__)


def _utc_now():
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
        validator: PlanValidator = DEFAULT_VALIDATOR,
    ) -> None:
        """
        Initialize the engine with adapters and a validator.

        Args:
            reader: Adapter that fetches the current catalog state.
            executor: Adapter that executes action plans.
            validator: Validator that checks plans for policy violations.

        """
        self.reader = reader
        self.executor = executor
        self.validator = validator

    def sync(self, registry: Registry) -> None:
        """
        Synchronize all registered tables to their desired state.

        Computes, validates, and executes plans for each table in the supplied
        registry. Raises on validation or execution failures with rich context.
        """
        run_started = _utc_now()
        tables = list(registry)
        logger.info("Starting sync for %d table(s)", len(tables))
        table_reports = [self._sync_table(t) for t in tables]

        report = SyncReport(
            started_at=run_started,
            ended_at=_utc_now(),
            table_reports=tuple(table_reports),
        )

        if report.any_failures:
            raise SyncFailedError(report)

        print(format_sync_report(report))
        logger.info("Sync completed successfully for %d table(s)", len(report.table_reports))

    def _sync_table(self, desired: DesiredTable) -> TableRunReport:
        """Synchronize a single table to its desired state."""
        started = _utc_now()
        fully_qualified_name = str(desired.qualified_name)
        logger.info("Processing table %s", fully_qualified_name)

        # --- Step 1: Read
        read_result = self.reader.fetch_state(desired.qualified_name)
        if read_result.failure:
            logger.warning(
                "Read failed for %s: %s - %s",
                fully_qualified_name,
                read_result.failure.exception_type,
                read_result.failure.message,
            )
            return TableRunReport(
                fully_qualified_name=fully_qualified_name,
                started_at=started,
                ended_at=_utc_now(),
                read=read_result,
                validation=ValidationResult(failures=()),
                execution_results=(),
            )

        observed = read_result.observed  # may be None if absent
        logger.info(
            "Read state for %s: %s",
            fully_qualified_name,
            "present" if observed is not None else "absent",
        )

        # --- Step 2: Plan
        context = make_plan_context(desired, observed)
        num_actions = len(context.plan)
        logger.info("Planned %d action(s) for %s", num_actions, fully_qualified_name)

        # --- Step 3: Validate
        validation_failures = self.validator.validate(context)
        validation = ValidationResult(failures=validation_failures)
        if validation.failed:
            logger.warning(
                "Validation failed for %s (%d failure(s))",
                fully_qualified_name,
                len(validation.failures),
            )
            return TableRunReport(
                fully_qualified_name=fully_qualified_name,
                started_at=started,
                ended_at=_utc_now(),
                read=read_result,
                validation=validation,
                execution_results=(),
            )

        # --- Step 4: Execute
        executions = self.executor.execute(context.plan)
        failed_execs = sum(1 for e in executions if e.failure is not None)
        logger.info(
            "Executed %d action(s) for %s (%d failed)",
            len(executions),
            fully_qualified_name,
            failed_execs,
        )

        return TableRunReport(
            fully_qualified_name=fully_qualified_name,
            started_at=started,
            ended_at=_utc_now(),
            read=read_result,
            validation=ValidationResult(failures=()),
            execution_results=executions,
        )
