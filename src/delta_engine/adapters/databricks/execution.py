"""
Shared statement-execution loop for the Databricks backends.

Both backends execute compiled SQL statements one at a time, stop at the
first failure, and record bounded single-line previews on the results. Only
two facts differ per backend — how a statement is physically executed, and
how its exceptions are translated — so both are injected as callables.
"""

from collections.abc import Callable, Iterable
import logging

from delta_engine.adapters.databricks.errors import ExceptionDetails
from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    ExecutionFailed,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
)

logger = logging.getLogger(__name__)


def execute_statements(
    execute: Callable[[str], object],
    statements: Iterable[str],
    translate: Callable[[Exception], ExceptionDetails],
) -> ExecutionSummary:
    """
    Execute each statement in order, stopping at the first failure.

    Execution stops at the first failure: the statements form a dependency
    chain, and the engine is not transactional, so continuing past a failure
    risks compounding a half-migrated table. The summary covers the
    statements attempted, ending at the one that failed; statements after it
    are left unattempted rather than run against an inconsistent table.
    """
    results: list[ExecutionResult] = []
    for statement_index, statement in enumerate(statements):
        result = _execute_statement(execute, statement_index, statement, translate)
        results.append(result)
        if isinstance(result, ExecutionFailed):
            break
    return ExecutionSummary(tuple(results))


def _execute_statement(
    execute: Callable[[str], object],
    statement_index: int,
    statement: str,
    translate: Callable[[Exception], ExceptionDetails],
) -> ExecutionResult:
    """
    Execute a single statement and map its outcome to an ``ExecutionResult``.

    The broad ``except`` is intentional: each backend raises a heterogeneous
    set of failures that varies across runtime environments, and the
    executor's contract is to wrap any failure in an ``ExecutionFailed`` so
    the run can record it and stop cleanly — never to let a backend-specific
    exception escape. Narrowing the catch would reintroduce silent
    propagation of whichever type was missed.
    """
    preview = sql_preview(statement)
    try:
        execute(statement)
    except Exception as exception:
        details = translate(exception)
        logger.warning("Statement failed: %s\nSQL: %s", details.message, preview)
        return ExecutionFailed(
            failure=ExecutionFailure(
                statement_index=statement_index,
                exception_type=details.type_name,
                message=details.message,
                statement_preview=preview,
            ),
        )

    logger.info("Executed: %s", preview)
    return ExecutionSucceeded(
        statement_index=statement_index,
        statement_preview=preview,
    )


def sql_preview(sql: str, *, max_chars: int = 240) -> str:
    """
    Return a compact, bounded preview of a SQL statement for logs/results.

    - Normalizes all runs of whitespace to single spaces on one line.
    - Truncates with an ellipsis when longer than max_chars.

    The bound and formatting are executor reporting policy — the preview
    lands in ``statement_preview`` on execution results and in log lines,
    never back in SQL sent to the backend.
    """
    normalized = " ".join(sql.split())
    return normalized if len(normalized) <= max_chars else (normalized[:max_chars] + "…")
