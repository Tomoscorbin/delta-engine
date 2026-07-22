"""Tests for failure handling in credentialed capability probes."""

from databricks.sql.exc import OperationalError, ServerOperationError
import pytest

from tests.live.capabilities import (
    databricks_error_condition,
    require_databricks_capability,
)

_QUOTA = "QUOTA_EXCEEDED_EXCEPTION"


def _server_error(condition: str, detail: str = "detail") -> ServerOperationError:
    return ServerOperationError(f"[{condition}] {detail}")


def test_error_condition_is_independent_of_message_prose():
    # Given two rendered errors with the same stable condition but different prose
    first = _server_error(_QUOTA, "the old explanation")
    second = _server_error(_QUOTA, "a localized or revised explanation")

    # When their conditions are extracted
    conditions = (databricks_error_condition(first), databricks_error_condition(second))

    # Then only the stable condition determines the result
    assert conditions == (_QUOTA, _QUOTA)


def test_structured_error_condition_is_preferred_when_available():
    # Given a connector error with a structured condition and unrelated rendered text
    error = ServerOperationError("rendered text without a condition prefix")
    error.error_code = _QUOTA

    # When its condition is extracted
    condition = databricks_error_condition(error)

    # Then the connector's structured field is used
    assert condition == _QUOTA


@pytest.mark.parametrize(
    "error",
    [
        _server_error("INSUFFICIENT_PERMISSIONS"),
        _server_error("PARSE_SYNTAX_ERROR"),
        OperationalError("warehouse connection failed"),
    ],
)
def test_unrecognized_failures_are_propagated(error: Exception):
    # Given a capability probe that raises a non-capability failure
    def operation() -> None:
        raise error

    # When the probe runs
    # Then the original failure escapes unchanged
    with pytest.raises(type(error)) as exc_info:
        require_databricks_capability(
            operation,
            capability="streaming tables",
            unavailable_conditions={"STREAMING_TABLE_NOT_SUPPORTED"},
        )

    assert exc_info.value is error


def test_recognized_unavailable_condition_skips_the_probe():
    # Given a probe rejected because the requested feature is unavailable
    def operation() -> None:
        raise _server_error("STREAMING_TABLE_NOT_SUPPORTED")

    # When the probe runs
    # Then pytest records a skip instead of a failure
    with pytest.raises(pytest.skip.Exception):
        require_databricks_capability(
            operation,
            capability="streaming tables",
            unavailable_conditions={"STREAMING_TABLE_NOT_SUPPORTED"},
        )


def test_retryable_condition_can_recover_before_the_deadline():
    # Given a quota-bound operation that succeeds on its third attempt
    attempts = 0
    now = 0.0
    delays: list[float] = []
    retries: list[tuple[str, float]] = []

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise _server_error(_QUOTA)

    def clock() -> float:
        return now

    def pause(delay: float) -> None:
        nonlocal now
        delays.append(delay)
        now += delay

    def report_retry(condition: str, delay: float) -> None:
        retries.append((condition, delay))

    # When the probe retries against a monotonic deadline
    require_databricks_capability(
        operation,
        capability="streaming tables",
        unavailable_conditions=set(),
        retry_conditions={_QUOTA},
        retry_timeout_seconds=5,
        retry_interval_seconds=2,
        clock=clock,
        pause=pause,
        report_retry=report_retry,
    )

    # Then it stops retrying as soon as the operation succeeds
    assert attempts == 3
    assert delays == [2, 2]
    assert retries == [(_QUOTA, 2), (_QUOTA, 2)]


def test_retryable_condition_skips_at_the_deadline():
    # Given a quota-bound operation that cannot succeed within its deadline
    attempts = 0
    now = 0.0
    delays: list[float] = []

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise _server_error(_QUOTA)

    def clock() -> float:
        return now

    def pause(delay: float) -> None:
        nonlocal now
        delays.append(delay)
        now += delay

    # When the deadline is exhausted
    # Then pytest records a skip without waiting beyond that deadline
    with pytest.raises(pytest.skip.Exception):
        require_databricks_capability(
            operation,
            capability="streaming tables",
            unavailable_conditions=set(),
            retry_conditions={_QUOTA},
            retry_timeout_seconds=5,
            retry_interval_seconds=2,
            clock=clock,
            pause=pause,
        )

    assert attempts > 1
    assert sum(delays) == 5
    assert all(0 < delay <= 2 for delay in delays)
