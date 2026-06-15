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
    PlanContext,
    make_plan_context,
)
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    ExecutionResult,
    ReadResult,
    SyncReport,
    TableRunReport,
    ValidationResult,
)
from delta_engine.application.validation import DEFAULT_VALIDATOR, PlanValidator
from delta_engine.domain.model.table import DesiredTable, ObservedTable
from delta_engine.log_config import configure_logging

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)  # do we need to DI the logger?


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
            validator: Validator that checks plans for plan violations.

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
        print(format_sync_report(report))  # TODO: figure out why some logging is printed after this

    def _sync_table(self, desired: DesiredTable) -> TableRunReport:
        """
        Synchronize a single table to its desired state.

        Runs the read -> plan -> validate -> execute pipeline, short-circuiting
        when a phase fails. Later phases leave their results at the empty
        default, so the report is assembled once from whatever each phase
        produced.
        """
        started = _utc_now()
        fully_qualified_name = str(desired.qualified_name)
        logger.info("Processing table %s", fully_qualified_name)

        validation = ValidationResult()
        executions: tuple[ExecutionResult, ...] = ()

        read_result = self._read(desired, fully_qualified_name)
        if not read_result.failure:
            context = self._plan(desired, read_result.observed, fully_qualified_name)
            validation = self._validate(context, fully_qualified_name)
            if not validation.failed:
                executions = self._execute(context, fully_qualified_name)

        return TableRunReport(
            fully_qualified_name=fully_qualified_name,
            started_at=started,
            ended_at=_utc_now(),
            read=read_result,
            validation=validation,
            execution_results=executions,
        )

    def _read(self, desired: DesiredTable, fully_qualified_name: str) -> ReadResult:
        """Read current catalog state, logging the outcome."""
        read_result = self.reader.fetch_state(desired.qualified_name)
        if read_result.failure:
            logger.error(
                "Read failed for %s: %s - %s",
                fully_qualified_name,
                read_result.failure.exception_type,
                read_result.failure.message,
            )
        else:
            logger.info(
                "Read state for %s: %s",
                fully_qualified_name,
                "present" if read_result.observed is not None else "absent",
            )
        return read_result

    def _plan(
        self,
        desired: DesiredTable,
        observed: ObservedTable | None,
        fully_qualified_name: str,
    ) -> PlanContext:
        """Compute the ordered action plan to reach the desired state."""
        context = make_plan_context(desired, observed)
        logger.info("Planned %d action(s) for %s", len(context.plan), fully_qualified_name)
        return context

    def _validate(self, context: PlanContext, fully_qualified_name: str) -> ValidationResult:
        """Validate the planned actions, logging the outcome."""
        logger.info("Validating plan for %s", fully_qualified_name)
        validation = ValidationResult(failures=self.validator.validate(context))
        if validation.failed:
            logger.error(
                "Validation failed for %s (%d failure(s))",
                fully_qualified_name,
                len(validation.failures),
            )
        else:
            logger.info("Validation passed for %s", fully_qualified_name)
        return validation

    def _execute(
        self, context: PlanContext, fully_qualified_name: str
    ) -> tuple[ExecutionResult, ...]:
        """Execute the planned actions, logging how many failed."""
        executions = self.executor.execute(context.plan)
        failed_executions = sum(1 for execution in executions if execution.failure is not None)
        logger.info(
            "Executed %d action(s) for %s (%d failed)",
            len(executions),
            fully_qualified_name,
            failed_executions,
        )
        return executions
