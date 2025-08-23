from __future__ import annotations

from tabula.application.change_target import load_change_target
from tabula.application.errors import ExecutionFailed
from tabula.application.plan.plan import preview_plan
from tabula.application.ports import CatalogReader, PlanExecutor
from tabula.application.results import ExecutionResult, PlanPreview
from tabula.application.validation import DEFAULT_VALIDATOR, PlanValidator
from tabula.domain.model import DesiredTable


def execute_plan(preview: PlanPreview, executor: PlanExecutor) -> ExecutionResult:
    """
    Execute a previously planned plan. Raises ExecutionFailed on any failure.
    Returns ExecutionResult on success.
    """
    outcome = executor.execute(preview.plan)
    if not outcome:
        raise ExecutionFailed(
            qualified_name=preview.plan.target,
            messages=outcome.messages,
            executed_count=outcome.executed_count,
        )
    return ExecutionResult(
        plan=preview.plan,
        messages=outcome.messages,
        executed_count=outcome.executed_count,
    )


def plan_then_execute(
    desired_table: DesiredTable,
    reader: CatalogReader,
    executor: PlanExecutor,
    validator: PlanValidator = DEFAULT_VALIDATOR,
) -> ExecutionResult:
    """
    Convenience orchestration: plan + (conditionally) execute.
    Returns a successful no-op result when nothing to do.
    """
    subject = load_change_target(reader, desired_table)
    preview = preview_plan(subject, validator)
    if not preview:
        return ExecutionResult(plan=preview.plan, messages=("noop",), executed_count=0)
    return execute_plan(preview, executor)
