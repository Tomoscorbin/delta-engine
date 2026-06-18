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
    ExecutionFailed,
    ExecutionFailure,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
)
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan

logger = logging.getLogger(__name__)


class DatabricksExecutor:
    """Plan executor that runs compiled statements via a Spark session."""

    def __init__(
        self,
        spark: SparkSession,
        compiler: Callable[[QualifiedName, ActionPlan], Iterable[str]] = compile_plan,
    ) -> None:
        self.spark = spark
        self._compiler = compiler

    def execute(self, target: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        """
        Execute the plan's actions against ``target`` and summarize the outcome.

        Execution stops at the first failure: the actions form a dependency
        chain, and the engine is not transactional, so continuing past a failure
        risks compounding a half-migrated table. The summary covers the actions
        attempted, ending at the one that failed; actions after it are left
        unattempted rather than run against an inconsistent table.
        """
        statements = self._compiler(target, plan)
        results: list[ExecutionResult] = []
        for action_index, statement in enumerate(statements):
            result = self._run_statement(plan[action_index], action_index, statement)
            results.append(result)
            if isinstance(result, ExecutionFailed):
                break
        return ExecutionSummary(tuple(results))

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
            return ExecutionFailed(
                action=action_name,
                action_index=action_index,
                statement_preview=preview,
                failure=ExecutionFailure(
                    action_index=action_index,
                    exception_type=type(exception).__name__,
                    message=error_preview(exception),
                ),
            )

        logger.info("Executed: %s", action_name)
        return ExecutionSucceeded(
            action=action_name,
            action_index=action_index,
            statement_preview=preview,
        )
