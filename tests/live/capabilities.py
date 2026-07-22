"""Capability handling shared by credentialed Databricks tests."""

from collections.abc import Callable, Collection
import re
import time

from databricks.sql.exc import ServerOperationError
import pytest

_CONDITION_NAME = r"[A-Z][A-Z0-9_]*(?:\.[A-Z][A-Z0-9_]*)*"
_RENDERED_CONDITION = re.compile(rf"^\[({_CONDITION_NAME})\]")

type RetryReporter = Callable[[str, float], None]


def databricks_error_condition(error: ServerOperationError) -> str | None:
    """Return the stable Databricks error condition, excluding mutable prose."""
    # The kernel transport exposes the condition directly. The Thrift transport
    # currently exposes the same condition only as the leading bracketed token.
    structured_condition = getattr(error, "error_code", None)
    if isinstance(structured_condition, str) and re.fullmatch(
        _CONDITION_NAME, structured_condition
    ):
        return structured_condition

    if not isinstance(error.message, str):
        return None
    match = _RENDERED_CONDITION.match(error.message)
    return match.group(1) if match else None


def require_databricks_capability(
    operation: Callable[[], None],
    *,
    capability: str,
    unavailable_conditions: Collection[str],
    retry_conditions: Collection[str] = (),
    retry_timeout_seconds: float = 0,
    retry_interval_seconds: float = 1,
    clock: Callable[[], float] = time.monotonic,
    pause: Callable[[float], None] = time.sleep,
    report_retry: RetryReporter | None = None,
) -> None:
    """Run a probe, skipping only named capability conditions after bounded retries."""
    deadline = clock() + retry_timeout_seconds

    while True:
        try:
            operation()
            return
        except ServerOperationError as error:
            condition = databricks_error_condition(error)
            now = clock()
            if condition in retry_conditions and now < deadline:
                delay = min(retry_interval_seconds, deadline - now)
                if report_retry is not None:
                    report_retry(condition, delay)
                pause(delay)
                continue
            if condition in unavailable_conditions or condition in retry_conditions:
                pytest.skip(f"{capability} is unavailable ({condition})")
            raise
