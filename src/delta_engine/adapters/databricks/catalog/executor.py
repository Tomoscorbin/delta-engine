"""
Execute compiled plans on Databricks/Spark and capture results.

Compiles an `ActionPlan` to SQL, runs each statement via a `SparkSession`, and
returns `ExecutionResult` entries including SQL previews and failure details.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
import logging

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.sql.compile import compile_plan
from delta_engine.adapters.databricks.sql.preview import error_preview, sql_preview
from delta_engine.application.results import (
    ActionStatus,
    ExecutionFailure,
    ExecutionResult,
)
from delta_engine.domain.plan.actions import ActionPlan

logger = logging.getLogger(__name__)


class DatabricksExecutor:
    """Plan executor that runs compiled statements via a Spark session."""

    def __init__(
        self,
        spark: SparkSession,
        compiler: Callable[[ActionPlan], Iterable[str]] = compile_plan,
    ) -> None:
        self.spark = spark
        self._compiler = compiler

    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        """
        Execute every action in the plan, returning a per-action result.

        Each statement runs to completion; a failure is captured in its own
        `ExecutionResult` rather than re-raised, so one failing action does not
        hide the outcome of the others.
        """
        statements = self._compiler(plan)
        return tuple(
            self._run_statement(plan[action_index], action_index, statement)
            for action_index, statement in enumerate(statements)
        )

    def _run_statement(self, action, action_index: int, statement: str) -> ExecutionResult:
        """Run a single compiled statement and map its outcome to an `ExecutionResult`."""
        action_name = type(action).__name__
        preview = sql_preview(statement)
        try:
            self.spark.sql(statement)
        except Exception as exception:
            logger.warning(
                "%s failed: %s\nSQL: %s",
                action_name,
                error_preview(exception),
                preview,
            )
            return ExecutionResult(
                action=action_name,
                action_index=action_index,
                status=ActionStatus.FAILED,
                statement_preview=preview,
                failure=ExecutionFailure(
                    action_index=action_index,
                    exception_type=type(exception).__name__,
                    message=error_preview(exception),
                ),
            )

        logger.info("Executed: %s", action_name)
        return ExecutionResult(
            action=action_name,
            action_index=action_index,
            status=ActionStatus.OK,
            statement_preview=preview,
        )
