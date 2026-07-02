"""
Execute compiled plans on Databricks/Spark and capture results.

Compiles an `ActionPlan` to SQL, runs each statement via a `SparkSession`, and
returns `ExecutionResult` entries including SQL previews and failure details.
"""

from __future__ import annotations

from collections.abc import Iterable
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
from delta_engine.domain.plan.actions import Action, ActionPlan

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
        statements = compile_plan(qualified_name, plan)
        return _execute_statements(self.spark, plan, statements)


def _execute_statements(
    spark: SparkSession, plan: ActionPlan, statements: Iterable[str]
) -> ExecutionSummary:
    """
    Run each compiled statement in plan order, stopping at the first failure.

    Holds the stop-on-first-failure loop as a free function so it is testable
    without a Spark session: a unit test passes a fake ``spark`` and a pre-canned
    ``statements`` list, with no need to inject a compiler.

    The compiler emits exactly one statement per action, in plan order, so zip
    pairs each action with its statement directly -- no positional index into the
    plan to keep in step with the loop counter. strict=True turns a compiler/plan
    length mismatch into a loud error rather than a silent truncation.
    """
    results: list[ExecutionResult] = []
    for action_index, (action, statement) in enumerate(zip(plan, statements, strict=True)):
        result = _run_statement(spark, action, action_index, statement)
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
    preview = sql_preview(statement)
    try:
        spark.sql(statement)
    except Exception as exception:
        error_message = error_preview(exception)
        logger.warning("%s failed: %s\nSQL: %s", action_name, error_message, preview)
        return ExecutionFailed(
            action=action_name,
            failure=ExecutionFailure(
                action_index=action_index,
                exception_type=exception_type_name(exception),
                message=error_message,
                statement_preview=preview,
            ),
        )

    logger.info("Executed: %s", action_name)
    return ExecutionSucceeded(
        action=action_name,
        action_index=action_index,
        statement_preview=preview,
    )
