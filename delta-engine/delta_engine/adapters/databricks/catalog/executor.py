from __future__ import annotations

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.preview import error_preview, sql_preview
from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.application.results import (
    ActionStatus,
    ExecutionFailure,
    ExecutionResult,
)
from delta_engine.domain.plan.actions import ActionPlan


class DatabricksExecutor:
    """Plan executor that runs compiled statements via a Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        """Execute all actions in the plan, returning per-action results."""
        if not plan:
            return ()

        statements = compile_plan(plan)
        results: list[ExecutionResult] = []
        for idx, (action, statement) in enumerate(
            zip(plan, statements, strict=True)  # feels too complicated
        ):
            res = self._run_action(
                action_name=type(action).__name__,
                action_index=idx,
                statement=statement,
            )
            results.append(res)

        return tuple(results)

    def _run_action(
        self, *, action_name: str, action_index: int, statement: str
    ) -> ExecutionResult:
        """Execute a single statement and return its result."""
        preview = sql_preview(statement)

        status: ActionStatus
        failure: ExecutionFailure | None = None
        try:
            self.spark.sql(statement)
            status = ActionStatus.OK
        except Exception as exc:
            status = ActionStatus.FAILED
            failure = ExecutionFailure(
                action_index=action_index,
                exception_type=type(exc).__name__,
                message=error_preview(exc),
            )

        return ExecutionResult(
            action=action_name,
            action_index=action_index,
            status=status,
            statement_preview=preview,
            failure=failure,
        )
