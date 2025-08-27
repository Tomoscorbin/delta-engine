from __future__ import annotations

from datetime import datetime, timezone
from collections.abc import Sequence
from pyspark.sql import SparkSession

from delta_engine.application.results import (
    TableExecutionReport,
    ExecutionFailure,
    ActionResult,
    ActionStatus,
)
from delta_engine.adapters.databricks.catalog.compile import compile_plan
from delta_engine.domain.plan.actions import ActionPlan


def _sql_preview(sql: str, limit: int = 200) -> str:
    one_line = " ".join(sql.split())
    return one_line if len(one_line) <= limit else f"{one_line[:limit]}…"

class DatabricksExecutor:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def execute_table(self, plan: ActionPlan) -> TableExecutionReport:
        statements: tuple[str] = compile_plan(plan)
        results: list[ActionResult] = []

        for index, statement in enumerate(statements):
            action_name = plan[index].__class__.__name__  
            execution_result = self._run_action(plan, action_name, index, statement)
            results.append(execution_result)

        return TableExecutionReport(
            fully_qualified_name=str(plan.target),
            total_actions=len(statements),
            results=tuple(results),
        )


    def _run_action(self, plan: ActionPlan, action_name: str, action_index: int, statement: str) -> ActionResult:
        """Execute a single statement and return its ActionResult (logs along the way)."""
        preview = _sql_preview(statement)
        try:
            self.spark.sql(statement)
        except Exception as exc:
            failure = self._build_failure(plan, action_index, exc, preview)
            return ActionResult(action_name, action_index, ActionStatus.FAILED, preview, failure)

        return ActionResult(action_name, action_index, ActionStatus.OK, preview)

    @staticmethod
    def _build_failure(
        plan: ActionPlan,
        action_index: int,
        exc: Exception,
        preview: str,
    ) -> ExecutionFailure:
        message_head = (str(exc) or type(exc).__name__)
        message_first_lines = "\n".join(message_head.splitlines()[:5])
        return ExecutionFailure(
            fully_qualified_name=str(plan.target),
            action_index=action_index,
            exception_type=type(exc).__name__,
            message=message_first_lines,
            statement_preview=preview,
        )

