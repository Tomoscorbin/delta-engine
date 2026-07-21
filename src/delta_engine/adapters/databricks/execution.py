"""Translate Databricks statement failures into the application error."""

from collections.abc import Callable
import logging

from delta_engine.adapters.databricks.exception_inspection import (
    exception_message,
    exception_type_name,
)
from delta_engine.application.errors import ExecutionError

logger = logging.getLogger(__name__)


def execute_statement(
    execute: Callable[[str], object],
    statement: str,
) -> None:
    """
    Execute one statement and normalize any backend exception.

    The broad catch is intentional: the two Databricks clients raise different
    exception families across runtime versions. This boundary translates all
    of them into one application-owned error while preserving their useful
    backend type and message. The application decides whether another
    statement should be attempted.

    Raises:
        ExecutionError: The backend could not execute the statement.

    """
    try:
        execute(statement)
    except Exception as exception:
        message = exception_message(exception)
        logger.warning("Statement failed: %s\nSQL: %s", message, statement)
        raise ExecutionError(
            exception_type=exception_type_name(exception),
            message=message,
        ) from exception

    logger.debug("Executed: %s", statement)
