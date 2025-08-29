from __future__ import annotations

from datetime import UTC, datetime

from delta_engine.application.errors import (
    SyncFailedError,
)
from delta_engine.application.plan import (
    make_plan_context,
)
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    SyncReport,
    TableRunReport,
)
from delta_engine.application.validation import DEFAULT_VALIDATOR, PlanValidator
from delta_engine.domain.model.table import DesiredTable
from delta_engine.log_config import configure_logging

configure_logging()


def _utc_now():
    return datetime.now(UTC).isoformat()


class Engine:
    """High-level orchestrator to plan, validate, and execute changes.

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
        """Initialize the engine with adapters and a validator.

        Args:
            reader: Adapter that fetches the current catalog state.
            executor: Adapter that executes action plans.
            validator: Validator that checks plans for policy violations.

        """
        self.reader = reader
        self.executor = executor
        self.validator = validator

    def sync(self, registry: Registry) -> None:
        """Synchronize all registered tables to their desired state.

        Computes, validates, and executes plans for each table in the supplied
        registry. Raises on validation or execution failures with rich context.
        """
        run_started = _utc_now()
        table_reports = [self._sync_table(t) for t in registry]

        run_report = SyncReport(
            started_at=run_started,
            ended_at=_utc_now(),
            tables=table_reports,
        )

        if run_report.any_failures:
            raise SyncFailedError(run_report)

        return run_report

    def _sync_table(self, desired: DesiredTable) -> TableRunReport:
        """Synchronize a single table to its desired state."""
        started = _utc_now()
        fully_qualified_name = str(desired.qualified_name)

        # --- Step 1: Read
        read_result = self.reader.fetch_state(desired.qualified_name)
        if read_result.failure:
            return TableRunReport(
                fully_qualified_name=fully_qualified_name,
                started_at=started,
                ended_at=_utc_now(),
                failure=(read_result.failure,),
            )

        observed = read_result.observed  # may be None if absent

        # --- Step 2: Plan
        context = make_plan_context(desired, observed)

        # --- Step 3: Validate
        failures = self.validator.validate(context)
        if failures:
            return TableRunReport(
                fully_qualified_name=fully_qualified_name,
                started_at=started,
                ended_at=_utc_now(),
                failure=failures,
            )

        # --- Step 4: Execute
        executions = self.executor.execute_table(context.plan)

        return TableRunReport(
            fully_qualified_name=fully_qualified_name,
            started_at=started,
            ended_at=_utc_now(),
            execution_results=executions,
        )
