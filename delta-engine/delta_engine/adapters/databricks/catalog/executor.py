"""
Execute compiled plans on Databricks/Spark and capture results.

Compiles an `ActionPlan` to SQL, runs each statement via a `SparkSession`, and
returns `ExecutionResult` entries including SQL previews and failure details.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
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
from delta_engine.log_config import configure_logging

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _AppliedStep:
    """Internal record of what happened when the statement is applied."""

    action_name: str
    action_index: int
    statement: str
    exception: Exception | None


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
        """Execute all actions in the plan, returning per-action results."""
        if not plan:
            return ()

        steps = self._apply(plan)
        return self._to_results(steps)

    def _apply(self, plan: ActionPlan) -> tuple[_AppliedStep, ...]:
        """
        Execute all statements for the plan.

        Raises:
            RuntimeError: if any statement fails.
            ValueError: if the compiler returns a mismatched count.

        """
        if not plan:
            return

        statements = self._compiler(plan)
        applied: list[_AppliedStep] = []

        for action_index, statement in enumerate(statements):
            action_name = type(plan[action_index]).__name__
            exception: Exception | None = None

            try:
                self.spark.sql(statement)
                logger.info("Executed: %s", action_name)
            except Exception as e:
                exception = e
                logger.warning(
                    "%s failed: %s\nSQL: %s",
                    action_name,
                    error_preview(exception),
                    sql_preview(statement),
                )

            applied.append(
                _AppliedStep(
                    action_name=action_name,
                    action_index=action_index,
                    statement=statement,
                    exception=exception,
                )
            )

        return tuple(applied)

    def _to_results(self, steps: tuple[_AppliedStep, ...]) -> tuple[ExecutionResult, ...]:
        results: list[ExecutionResult] = []
        for step in steps:
            preview = sql_preview(step.statement)
            if step.exception is None:
                status = ActionStatus.OK
                failure = None
            else:
                status = ActionStatus.FAILED
                failure = ExecutionFailure(
                    action_index=step.action_index,
                    exception_type=type(step.exception).__name__,
                    message=error_preview(step.exception),
                )

            results.append(
                ExecutionResult(
                    action=step.action_name,
                    action_index=step.action_index,
                    status=status,
                    statement_preview=preview,
                    failure=failure,
                )
            )
        return tuple(results)
