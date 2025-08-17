from __future__ import annotations
from tabula.application.ports import CatalogReader, PlanExecutor
from tabula.application.results import ExecutionResult, ExecutionOutcome
from tabula.application.errors import ExecutionFailed
from tabula.domain.model.table_spec import TableSpec
from tabula.application.plan import plan_actions

def execute_plan(
    spec: TableSpec,
    reader: CatalogReader,
    executor: PlanExecutor,
) -> ExecutionResult:
    plan_result = plan_actions(spec, reader)

    outcome: ExecutionOutcome = executor.execute(plan_result.plan)
    if not outcome.success:
        raise ExecutionFailed("; ".join(outcome.messages or ("execution failed",)))

    return ExecutionResult(plan=plan_result.plan, success=True, messages=outcome.messages)
