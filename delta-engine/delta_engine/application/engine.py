from __future__ import annotations

from datetime import UTC, datetime
import logging

from delta_engine.application.errors import (
    ExecutionFailedError,
    ValidationFailedError,
)
from delta_engine.application.plan import (
    PlanContext,
    make_plan_context,
)
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import ExecutionResult, RunReport, ValidationFailure
from delta_engine.application.validation import DEFAULT_VALIDATOR, PlanValidator
from delta_engine.log_config import configure_logging

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

        # 1) Plans for all tables
        contexts = self._build_contexts(registry)

        # # 2) Validate all plans
        self._validate_all(contexts)

        # 3) Execute all plans
        self._execute_all(contexts)


    def _build_contexts(self, registry: Registry) -> tuple[PlanContext, ...]:
        contexts: list[PlanContext] = []
        for desired in registry:
            observed = self.reader.fetch_state(desired.qualified_name)
            context = make_plan_context(desired=desired, observed=observed)
            contexts.append(context)
        return contexts

    def _validate_all(self, contexts: tuple[PlanContext, ...]) -> None:
        failures_by_table: dict[str, tuple[ValidationFailure,...]] = {}

        for ctx in contexts:
            fully_qualified_name = str(ctx.desired.qualified_name)
            failures = self.validator.validate(ctx)
            if failures:
                failures_by_table[fully_qualified_name] = failures

        if failures_by_table:
            raise ValidationFailedError(failures_by_table)


    def _execute_all(self, contexts: tuple[PlanContext, ...]) -> None:
        run_started = datetime.now(UTC).isoformat()
        executions_by_table: dict[str, tuple[ExecutionResult, ...]] = {}

        for ctx in contexts:
            fully_qualified_name = str(ctx.desired.qualified_name)
            executions = self.executor.execute_table(ctx.plan)
            executions_by_table[fully_qualified_name] = executions

        report = RunReport(
            run_id=datetime.now(UTC).strftime("%Y%m%dT%H%M%S"),
            started_at=run_started,
            ended_at=datetime.now(UTC).isoformat(),
            executions_by_table=executions_by_table,
        )

        if report.any_failures():
            raise ExecutionFailedError(report)

