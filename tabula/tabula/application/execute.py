"""Plan preview and execution helpers."""

from __future__ import annotations

from tabula.application.change_target import load_change_target
from tabula.application.errors import ExecutionFailedError
from tabula.application.plan.plan import preview_plan
from tabula.application.ports import CatalogReader, PlanExecutor
from tabula.application.results import ExecutionResult, PlanPreview
from tabula.application.validation import DEFAULT_VALIDATOR, PlanValidator
from tabula.domain.model import DesiredTable


def execute_plan(preview: PlanPreview, executor: PlanExecutor) -> ExecutionResult:
    """Execute a previously planned plan.

    Args:
        preview: Planned actions to execute.
        executor: Adapter responsible for running the plan.

    Returns:
        Result of the execution.

    Raises:
        ExecutionFailedError: If the executor reports failure.
    """

    outcome = executor.execute(preview.plan)
    if not outcome:
        raise ExecutionFailedError(
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
    """Plan a change and, if necessary, execute it.

    Args:
        desired_table: Target table definition.
        reader: Catalog reader adapter.
        executor: Plan executor adapter.
        validator: Plan validator to enforce policy.

    Returns:
        Execution result. If no actions are needed, ``executed_count`` is ``0``.
    """

    subject = load_change_target(reader, desired_table)
    preview = preview_plan(subject, validator)
    if not preview:
        return ExecutionResult(plan=preview.plan, messages=("noop",), executed_count=0)
    return execute_plan(preview, executor)
