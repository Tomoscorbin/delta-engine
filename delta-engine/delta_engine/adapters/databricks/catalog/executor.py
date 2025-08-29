from __future__ import annotations

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.catalog.compile import compile_plan
from delta_engine.adapters.databricks.preview import error_preview, sql_preview
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

    def execute_table(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        """Execute all actions in the plan, returning per-action results."""
        results: list[ExecutionResult] = []
        statements = compile_plan(plan)

        for action_index, (action, statement) in enumerate(
            zip(plan, statements, strict=False)
        ):  # feels a bit too complicated
            action_name = type(action).__name__
            execution_result = self._run_action(
                action_name=action_name,
                action_index=action_index,
                statement=statement,
            )
            results.append(execution_result)

        return tuple(results)

    def _run_action(
        self, *, action_name: str, action_index: int, statement: str
    ) -> ExecutionResult:
        """Execute a single statement and return its ActionResult (logs along the way)."""
        preview = sql_preview(statement)
        try:
            self.spark.sql(statement)
        except Exception as exc:
            exception_type = type(exc).__name__
            message = error_preview(exc)

            failure = ExecutionFailure(
                action_index=action_index,
                exception_type=exception_type,
                message=message,
            )

            return ExecutionResult(
                action=action_name,
                action_index=action_index,
                status=ActionStatus.FAILED,
                statement_preview=preview,
                failure=failure,
            )

        return ExecutionResult(  # TODO: have one return instead of two
            action=action_name,
            action_index=action_index,
            status=ActionStatus.OK,
            statement_preview=preview,
        )
