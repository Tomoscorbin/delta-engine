"""
Shared statement-execution loop for the Databricks backends.

Both backends execute compiled SQL statements one at a time, stop at the
first failure, and record each statement verbatim on its result. Only two
facts differ per backend — how a statement is physically executed, and how
a failing exception's type is named — so both are injected as callables,
the latter defaulting to the Python class name.
"""

from collections.abc import Callable, Iterable
import logging

from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    ExecutionFailed,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
)

logger = logging.getLogger(__name__)


def _python_exception_type_name(exception: Exception) -> str:
    """Name an exception by its Python class — the informative default outside py4j."""
    return type(exception).__name__


def execute_statements(
    execute: Callable[[str], object],
    statements: Iterable[str],
    exception_type_name: Callable[[Exception], str] = _python_exception_type_name,
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
        result = _execute_statement(execute, statement_index, statement, exception_type_name)
        results.append(result)
        if isinstance(result, ExecutionFailed):
            break
    return ExecutionSummary(tuple(results))


def _execute_statement(
    execute: Callable[[str], object],
    statement_index: int,
    statement: str,
    exception_type_name: Callable[[Exception], str],
) -> ExecutionResult:
    """
    Execute a single statement and map its outcome to an ``ExecutionResult``.

    The statement and any exception message are recorded verbatim — results
    are what users debug from, so nothing is normalized or truncated here;
    bounding long text is display policy, owned by the failure renderers.

    The broad ``except`` is intentional: each backend raises a heterogeneous
    set of failures that varies across runtime environments, and the
    executor's contract is to wrap any failure in an ``ExecutionFailed`` so
    the run can record it and stop cleanly — never to let a backend-specific
    exception escape. Narrowing the catch would reintroduce silent
    propagation of whichever type was missed.
    """
    try:
        execute(statement)
    except Exception as exception:
        logger.warning("Statement failed: %s\nSQL: %s", exception, statement)
        return ExecutionFailed(
            failure=ExecutionFailure(
                statement_index=statement_index,
                exception_type=exception_type_name(exception),
                message=str(exception),
                statement=statement,
            ),
        )

    # Per-statement narration is trace detail; the engine already reports
    # per-table progress at INFO.
    logger.debug("Executed: %s", statement)
    return ExecutionSucceeded(
        statement_index=statement_index,
        statement=statement,
    )
