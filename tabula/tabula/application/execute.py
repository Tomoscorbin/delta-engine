"""Plan execution orchestration with a clear failure policy."""

from __future__ import annotations

from tabula.application.errors import ExecutionFailed
from tabula.application.plan import plan_actions
from tabula.application.ports import CatalogReader, PlanExecutor
from tabula.application.results import ExecutionResult, PlanPreview
from tabula.domain.model.table import DesiredTable


def execute_plan(preview: PlanPreview, executor: PlanExecutor) -> ExecutionResult:
    """
    Execute a previously planned plan. Raises ExecutionFailed on any failure.
    Returns ExecutionResult on success.
    """
    outcome = executor.execute(preview.plan)
    if not outcome:
        raise ExecutionFailed(
            qualified_name=str(preview.plan.qualified_name),
            messages=outcome.messages,
            executed_count=outcome.executed_count,
        )
    return ExecutionResult(
        plan=preview.plan,
        messages=outcome.messages,
        executed_count=outcome.executed_count,
    )


def plan_then_execute(
    desired_table: DesiredTable, reader: CatalogReader, executor: PlanExecutor
) -> ExecutionResult:
    """
    Convenience orchestration: plan + (conditionally) execute.
    Returns a successful no-op result when nothing to do.
    """
    preview = plan_actions(desired_table, reader)
    if preview.is_noop:
        return ExecutionResult(plan=preview.plan, messages=("noop",), executed_count=0)
    return execute_plan(preview, executor)
