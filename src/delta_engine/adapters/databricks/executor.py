"""
Execute compiled plans on Databricks/Spark and capture results.

Compiles an `ActionPlan` to SQL, runs each statement via a `SparkSession`, and
returns `ExecutionResult` entries including SQL previews and failure details.
"""

from __future__ import annotations

from collections.abc import Iterable
import logging

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.errors import summarize_exception
from delta_engine.adapters.databricks.sql import (
    CompiledAction,
    compile_plan,
)
from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    ExecutionFailed,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
)
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import Action, ActionPlan

logger = logging.getLogger(__name__)


class DatabricksExecutor:
    """Plan executor that runs compiled statements via a Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the executor with the Spark session it runs statements on."""
        self.spark = spark

    def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        """
        Execute the plan's actions against ``qualified_name`` and summarize the outcome.

        Execution stops at the first failure: the actions form a dependency
        chain, and the engine is not transactional, so continuing past a failure
        risks compounding a half-migrated table. The summary covers the actions
        attempted, ending at the one that failed; actions after it are left
        unattempted rather than run against an inconsistent table.
        """
        return _execute_compiled(self.spark, compile_plan(qualified_name, plan))


def _execute_compiled(spark: SparkSession, compiled: Iterable[CompiledAction]) -> ExecutionSummary:
    """
    Run each compiled action in plan order, stopping at the first failure.

    Holds the stop-on-first-failure loop as a free function so it is testable
    without a Spark session: a unit test passes a fake ``spark`` and pre-built
    ``CompiledAction`` pairs, with no need to inject a compiler.
    """
    results: list[ExecutionResult] = []
    for action_index, compiled_action in enumerate(compiled):
        result = _run_statement(
            spark, compiled_action.action, action_index, compiled_action.statement
        )
        results.append(result)
        if isinstance(result, ExecutionFailed):
            break
    return ExecutionSummary(tuple(results))


def _run_statement(
    spark: SparkSession, action: Action, action_index: int, statement: str
) -> ExecutionResult:
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
    preview = _sql_preview(statement)
    try:
        spark.sql(statement)
    except Exception as exception:
        summary = summarize_exception(exception)
        logger.warning("%s failed: %s\nSQL: %s", action_name, summary.message, preview)
        return ExecutionFailed(
            action=action_name,
            failure=ExecutionFailure(
                action_index=action_index,
                exception_type=summary.type_name,
                message=summary.message,
                statement_preview=preview,
            ),
        )

    logger.info("Executed: %s", action_name)
    return ExecutionSucceeded(
        action=action_name,
        action_index=action_index,
        statement_preview=preview,
    )


def _sql_preview(sql: str, *, max_chars: int = 240) -> str:
    """
    Return a compact, bounded preview of a SQL statement for logs/results.

    - Normalizes all runs of whitespace to single spaces on one line.
    - Truncates with an ellipsis when longer than max_chars.

    The bound and formatting are this executor's reporting policy — the preview
    lands in ``statement_preview`` on execution results and in log lines, never
    back in SQL sent to Spark.
    """
    s = " ".join(sql.split())
    return s if len(s) <= max_chars else (s[:max_chars] + "…")
