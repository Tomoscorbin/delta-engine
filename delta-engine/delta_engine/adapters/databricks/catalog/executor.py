from __future__ import annotations

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.catalog.compile import compile_plan
from delta_engine.application.results import (
    ActionStatus,
    ExecutionFailure,
    ExecutionResult,
)
from delta_engine.domain.plan.actions import ActionPlan


def _sql_preview(sql: str, limit: int = 200) -> str:  # does this belong here?
    """Return a single-line preview of a SQL statement, truncated to ``limit``."""
    one_line = " ".join(sql.split())
    return one_line if len(one_line) <= limit else f"{one_line[:limit]}…"

class DatabricksExecutor:
    """Plan executor that runs compiled statements via a Spark session."""
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def execute_table(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        """Execute all actions in the plan, returning per-action results."""
        results: list[ExecutionResult] = []
        statements = compile_plan(plan)

        for action_index, (action, statement) in enumerate(zip(plan, statements, strict=False)): # feels a bit too complicated
            action_name = type(action).__name__
            execution_result = self._run_action(
                action_name=action_name,
                action_index=action_index,
                statement=statement,
            )
            results.append(execution_result)

        return tuple(results)


    def _run_action(self, *, action_name: str, action_index: int, statement: str) -> ExecutionResult:
        """Execute a single statement and return its ActionResult (logs along the way)."""
        preview = _sql_preview(statement)
        try:
            self.spark.sql(statement)
        except Exception as exc:
            exception_type = type(exc).__name__
            message_head = str(exc) or exception_type
            message_first_lines = "\n".join(message_head.splitlines()[:5])

            failure = ExecutionFailure(
                action_index=action_index,
                exception_type=exception_type,
                message=message_first_lines,
            )

            return ExecutionResult(
                action=action_name,
                action_index=action_index,
                status=ActionStatus.FAILED,
                statement_preview=preview,
                failure=failure,
            )

        return ExecutionResult(         # TODO: have one return instead of two
            action=action_name,
            action_index=action_index,
            status=ActionStatus.OK,
            statement_preview=preview,
        )
