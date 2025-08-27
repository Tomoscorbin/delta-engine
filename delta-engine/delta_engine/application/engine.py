from __future__ import annotations

import logging

from datetime import datetime
from typing import Iterable, Mapping

from delta_engine.application.errors import (
    ExecutionFailedError,
    ValidationFailedError,
)
from delta_engine.application.plan import (
    PlanContext,
    make_plan_context,
    plan_summary_counts,
)
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import PlanPreview, RunReport, TableValidationReport, ValidationRunReport
from delta_engine.application.validation import DEFAULT_VALIDATOR, PlanValidator, ValidationFailure
from delta_engine.log_config import configure_logging
from delta_engine.application.report_format import format_run_report



configure_logging()
_LOGGER = logging.getLogger(__name__)


class Engine:
    def __init__(
        self,
        reader: CatalogStateReader,
        executor: PlanExecutor,
        validator: PlanValidator = DEFAULT_VALIDATOR,
    ) -> None:
        self.reader = reader
        self.executor = executor
        self.validator = validator

    def sync(self, registry: Registry) -> None:
        """Two-phase run:
        1) Create plans and validate all tables. If any validation fails, raise
            ValidationFailedError (no execution happens).
        2) Execute all plans. If any execution fails, raise
            ExecutionFailedError (after attempting all tables).
        3) If both phases are clean, return SyncReport(previews, executions).
        """
        # 1) Plans for all tables
        contexts = self._build_contexts(registry)

        # # 2) Validate all plans
        self._validate_all(contexts)

        # 3) Execute all plans
        self._execute_all(contexts)


    def _build_contexts(self, registry: Registry) -> dict[str, PlanContext]:
        """Build contexts for every desired table.
        Assumes the plan factory returns a *sorted* ActionPlan.
        """
        contexts: dict[str, PlanContext] = {}
        for desired in registry:
            name = str(desired.qualified_name)
            observed = self.reader.fetch_state(desired.qualified_name)
            contexts[name] = make_plan_context(desired=desired, observed=observed)
        return contexts
    
    def _validate_all(self, contexts: dict[str, PlanContext]) -> None:
        started = datetime.utcnow().isoformat()
        validations: dict[str, TableValidationReport] = {}

        for name, ctx in contexts.items():
            failures = self.validator.validate(ctx)
            validations[name] = TableValidationReport(
                fully_qualified_name=name,
                failures=failures,
            )

        report = ValidationRunReport(
            run_id=datetime.utcnow().strftime("%Y%m%dT%H%M%S"),
            validations=validations,
        )

        if report.any_failures():
            raise ValidationFailedError(report)


    def _execute_all(self, contexts) -> None:
        run_started = datetime.utcnow().isoformat()
        executions: dict[str, TableExecutionReport] = {}

        for table_name, ctx in contexts.items():
            executions[table_name] = self.executor.execute_table(ctx.plan)

        report = RunReport(
            run_id=datetime.utcnow().strftime("%Y%m%dT%H%M%S"),
            started_at=run_started,
            ended_at=datetime.utcnow().isoformat(),
            executions=executions,
        )

        _LOGGER.info("\n" + format_run_report(report))

        if report.any_failures():
            raise ExecutionFailedError(report)

