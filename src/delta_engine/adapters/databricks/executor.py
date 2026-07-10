"""
Execute compiled statements on Databricks/Spark and capture results.

Compiles an `ActionPlan` to SQL statements, runs each via a `SparkSession`, and
returns `ExecutionResult` entries including SQL previews and failure details.
"""

from collections.abc import Iterable
import logging

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.errors import summarize_exception
from delta_engine.adapters.databricks.sql import compile_plan
from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    ExecutionFailed,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
)
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan

logger = logging.getLogger(__name__)


class DatabricksExecutor:
    """Plan executor that compiles plans to SQL and runs them via a Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def compile(self, qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
        """Compile ``plan`` to its SQL statements in execution order, without touching Spark."""
        return tuple(compiled.statement for compiled in compile_plan(qualified_name, plan))

    def execute(self, statements: tuple[str, ...]) -> ExecutionSummary:
        """
        Run each statement in order and summarize the outcome.

        Execution stops at the first failure: the statements form a dependency
        chain, and the engine is not transactional, so continuing past a failure
        risks compounding a half-migrated table. The summary covers the
        statements attempted, ending at the one that failed; statements after it
        are left unattempted rather than run against an inconsistent table.
        """
        return _execute_statements(self.spark, statements)


def _execute_statements(spark: SparkSession, statements: Iterable[str]) -> ExecutionSummary:
    """
    Run each statement in order, stopping at the first failure.

    Holds the stop-on-first-failure loop as a free function so it is testable
    without a Spark session: a unit test passes a fake ``spark`` and pre-built
    statements, with no need to inject a compiler.
    """
    results: list[ExecutionResult] = []
    for statement_index, statement in enumerate(statements):
        result = _run_statement(spark, statement_index, statement)
        results.append(result)
        if isinstance(result, ExecutionFailed):
            break
    return ExecutionSummary(tuple(results))


def _run_statement(spark: SparkSession, statement_index: int, statement: str) -> ExecutionResult:
    """
    Run a single statement and map its outcome to an `ExecutionResult`.

    The broad ``except`` is intentional and mirrors the reader's ``fetch_state``:
    Spark raises a heterogeneous set of failures (``Py4JJavaError``,
    ``AnalysisException``, and plain Python errors) that varies across runtime
    environments. The executor's contract is to wrap any failure in an
    ``ExecutionFailed`` so the run can record it and stop cleanly, never to let
    a backend-specific exception escape. Narrowing the catch would reintroduce
    silent propagation of whichever type was missed.
    """
    preview = _sql_preview(statement)
    try:
        spark.sql(statement)
    except Exception as exception:
        summary = summarize_exception(exception)
        logger.warning("Statement failed: %s\nSQL: %s", summary.message, preview)
        return ExecutionFailed(
            failure=ExecutionFailure(
                action_index=statement_index,
                exception_type=summary.type_name,
                message=summary.message,
                statement_preview=preview,
            ),
        )

    logger.info("Executed: %s", preview)
    return ExecutionSucceeded(
        action_index=statement_index,
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
    normalized = " ".join(sql.split())
    return normalized if len(normalized) <= max_chars else (normalized[:max_chars] + "…")
