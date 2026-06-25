"""
Execute compiled plans on Databricks/Spark and capture results.

Compiles an `ActionPlan` to SQL, runs each statement via a `SparkSession`, and
returns `ExecutionResult` entries including SQL previews and failure details.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
import logging

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.sql import (
    compile_plan,
    error_preview,
    exception_type_name,
    sql_preview,
)
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

    def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        """
        Execute the plan's actions against ``qualified_name`` and summarize the outcome.

        Execution stops at the first failure: the actions form a dependency
        chain, and the engine is not transactional, so continuing past a failure
        risks compounding a half-migrated table. The summary covers the actions
        attempted, ending at the one that failed; actions after it are left
        unattempted rather than run against an inconsistent table.
        """
        statements = self._compiler(qualified_name, plan)
        results: list[ExecutionResult] = []
        # The compiler emits exactly one statement per action, in plan order, so
        # zip pairs each action with its statement directly -- no positional index
        # into the plan to keep in step with the loop counter. strict=True turns a
        # compiler/plan length mismatch into a loud error rather than a silent
        # truncation.
        for action_index, (action, statement) in enumerate(zip(plan, statements, strict=True)):
            result = self._run_statement(action, action_index, statement)
            results.append(result)
            if isinstance(result, ExecutionFailed):
                break
        return ExecutionSummary(tuple(results))

    def _run_statement(self, action, action_index: int, statement: str) -> ExecutionResult:
        """
        Run a single compiled statement and map its outcome to an `ExecutionResult`.

        The broad ``except`` is intentional and mirrors the reader's ``fetch_state``:
        Spark raises a heterogeneous set of failures (``Py4JJavaError``,
        ``AnalysisException``, and plain Python errors) that varies across runtime
        environments. The executor's contract is to wrap any failure in an
        ``ExecutionFailed`` so the run can record it and stop cleanly, never to let
        a backend-specific exception escape. Narrowing the catch would reintroduce
        silent propagation of whichever type was missed.
        """
        action_name = type(action).__name__
        preview = sql_preview(statement)
        try:
            self.spark.sql(statement)
        except Exception as exception:
            err = error_preview(exception)
            logger.warning("%s failed: %s\nSQL: %s", action_name, err, preview)
            return ExecutionFailed(
                action=action_name,
                action_index=action_index,
                statement_preview=preview,
                failure=ExecutionFailure(
                    action_index=action_index,
                    exception_type=exception_type_name(exception),
                    message=err,
                    statement_preview=preview,
                ),
            )

        logger.info("Executed: %s", action_name)
        return ExecutionSucceeded(
            action=action_name,
            action_index=action_index,
            statement_preview=preview,
        )
