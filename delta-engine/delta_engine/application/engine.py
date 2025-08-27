from __future__ import annotations

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

    def sync(self, registry: Registry) -> SyncReport:
        """Two-phase run:
        1) Create plans and validate all tables. If any validation fails, raise
            ValidationFailedError (no execution happens).
        2) Execute all plans. If any execution fails, raise
            ExecutionFailedError (after attempting all tables).
        3) If both phases are clean, return SyncReport(previews, executions).
        """
        # 1) Plans for all tables
        contexts = self._build_contexts(registry)

        # 2) Validate ALL plans
        previews = self._build_previews(contexts)
        self._validate_all(contexts)

        # 3) Execute ALL plans
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

    def _build_previews(self, contexts: PlanContext) -> dict[str, PlanPreview]:
        """Build a PlanPreview for each table."""
        previews: dict[str, PlanPreview] = {}
        for name, ctx in contexts.items():
            plan = ctx.plan
            previews[name] = PlanPreview(
                plan=plan,
                is_noop=(len(plan.actions) == 0),
                summary_counts=plan_summary_counts(plan),
                total_actions=len(plan.actions),
            )
        return previews
    
    def _validate_all(self, contexts: dict[str, PlanContext]) -> ValidationRunReport:
        started = datetime.utcnow().isoformat()
        validations: dict[str, TableValidationReport] = {}

        for name, ctx in contexts.items():
            table_started = datetime.utcnow().isoformat()
            failures = self.validator.validate(ctx)
            validations[name] = TableValidationReport(
                fully_qualified_name=name,
                failures=failures,
                started_at=table_started,
                ended_at=datetime.utcnow().isoformat(),
            )

        validation_report = ValidationRunReport(
            run_id=datetime.utcnow().strftime("%Y%m%dT%H%M%S"),
            started_at=started,
            ended_at=datetime.utcnow().isoformat(),
            validations=validations,
        )

        if validation_report.any_failures():
            raise ValidationFailedError(validation_report)

        print(validation_report)


    def _execute_all(self, contexts) -> RunReport:
        run_started = datetime.utcnow().isoformat()
        executions: dict[str, TableExecutionReport] = {}

        for table_name, ctx in contexts.items():
            executions[table_name] = self.executor.execute_table(ctx.plan)

        run_report = RunReport(
            run_id=datetime.utcnow().strftime("%Y%m%dT%H%M%S"),
            started_at=run_started,
            ended_at=datetime.utcnow().isoformat(),
            executions=executions,
        )

        if run_report.any_failures():
            raise ExecutionFailedError(run_report)

        print(run_report)
