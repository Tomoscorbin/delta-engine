from __future__ import annotations

from tabula.application.errors import (
    ExecutionFailedError,
    ValidationFailedError,
)
from tabula.application.plan import (
    PlanContext,
    make_plan_context,
    plan_summary_counts,
)
from tabula.application.ports import CatalogStateReader, PlanExecutor
from tabula.application.registry import Registry
from tabula.application.results import ExecutionReport, PlanPreview, SyncReport
from tabula.application.validation import DEFAULT_VALIDATOR, PlanValidator, ValidationFailure


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
        validation_failures = self._validate_all(contexts)
        if validation_failures:
            partial = SyncReport(previews=previews, executions={})
            raise ValidationFailedError(
                failures_by_table=validation_failures,
                report=partial,
            )

        # 3) Execute ALL plans
        executions, exec_failures = self._execute_all(contexts)
        if exec_failures:
            partial = SyncReport(previews=previews, executions=executions)
            raise ExecutionFailedError(
                failures_by_table=exec_failures,
                report=partial,
            )

        # 4) Final report
        return SyncReport(previews=previews, executions=executions)

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

    def _validate_all(self, contexts: dict[str, PlanContext]) -> dict[str, tuple[ValidationFailure, ...]]:
        """Run validation for every context, collect all failures by table name."""
        failures: dict[str, tuple[ValidationFailure, ...]] = {}
        for name, ctx in contexts.items():
            table_failures = self.validator.evaluate(ctx)
            if table_failures:
                failures[name] = table_failures
        return failures

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

    def _execute_all(self, contexts):
        """Execute every plan.
        Returns (executions, failures) by table name.
        """
        executions: dict[str, ExecutionReport] = {}
        failures: dict[str, str] = {}

        for name, ctx in contexts.items():
            try:
                executions[name] = self.executor.execute(ctx.plan)
            except Exception as exc:
                failures[name] = str(exc)
        return executions, failures
