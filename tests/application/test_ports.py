import pytest

from delta_engine.application.failures import ExecutionFailure
from delta_engine.application.ports import (
    ExecutionSucceeded,
    ExecutionSummary,
)


def _ok_exec(idx=0, preview="ALTER TABLE ..."):
    return ExecutionSucceeded(statement_index=idx, statement=preview)


def _failed_exec(idx=0, preview="ALTER TABLE ...", exc="ValueError", msg="boom"):
    return ExecutionFailure(
        statement_index=idx,
        exception_type=exc,
        message=msg,
        statement=preview,
    )


def test_execution_summary_reports_no_failure_when_every_statement_succeeds():
    # Given a run whose statements all executed
    summary = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

    # Then the summary reports success with no failures
    assert not summary.failures
    assert summary.applied_count == 2


def test_execution_summary_exposes_the_failures_among_mixed_results():
    # Given a run of two statements, the second of which failed
    summary = ExecutionSummary((_ok_exec(0), _failed_exec(1, msg="bang")))

    # Then the summary surfaces the single failure and the applied count
    assert tuple(f.message for f in summary.failures) == ("bang",)
    assert summary.applied_count == 1


def test_execution_summary_defaults_to_an_empty_unattempted_run():
    # Given no execution happened (e.g. an earlier phase short-circuited)
    summary = ExecutionSummary()

    # Then it is an empty, non-failing summary
    assert summary.results == ()
    assert not summary.failures
    assert summary.applied_count == 0


def test_execution_summary_rejects_non_contiguous_statement_indexes():
    with pytest.raises(ValueError, match="contiguous"):
        ExecutionSummary((_ok_exec(1),))


def test_execution_summary_rejects_results_after_a_failure():
    with pytest.raises(ValueError, match="after a failure"):
        ExecutionSummary((_failed_exec(0), _ok_exec(1)))
