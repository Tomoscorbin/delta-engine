import pytest

from delta_engine.application.failures import ExecutionFailure, ReadFailure
from delta_engine.application.ports import (
    ExecutionSucceeded,
    ExecutionSummary,
    ReadResult,
    TableAbsent,
    TablePresent,
)
from delta_engine.domain.model import Integer, ObservedColumn, ObservedTable, QualifiedName

# ---------- test builders


def _an_observed_table(partitioned_by=()):
    """Build a real ObservedTable, so reports are exercised against the domain type."""
    return ObservedTable(
        qualified_name=QualifiedName("cat", "schema", "observed"),
        columns=(ObservedColumn("id", Integer()),),
        partitioned_by=partitioned_by,
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


# ---------- Tests


def test_present_state_holds_the_observed_table():
    # Given a table that was read and exists
    observed = _an_observed_table()

    # When recording its catalog state
    state = TablePresent(table=observed)

    # Then it carries the observed table
    assert state.table is observed


def test_absent_state_is_distinct_from_a_failure():
    # Given a table that was read but does not exist

    # When recording its catalog state
    state = TableAbsent()

    # Then absence is its own state
    assert isinstance(state, TableAbsent)


def test_read_result_retains_the_failure_without_a_wrapper():
    # Given a read that raised
    failure = ReadFailure(exception_type="RuntimeError", message="catalog unreachable")

    # When recording a failed read
    result: ReadResult = failure

    # Then the failure itself is the result
    assert result is failure


def test_execution_summary_reports_no_failure_when_every_statement_succeeds():
    # Given a run whose statements all executed
    summary = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

    # Then the summary reports success with no failures
    assert summary.failed is False
    assert summary.failures == ()
    assert summary.failed_count == 0
    assert summary.applied_count == 2


def test_execution_summary_exposes_the_failures_among_mixed_results():
    # Given a run of two statements, the second of which failed
    summary = ExecutionSummary((_ok_exec(0), _failed_exec(1, msg="bang")))

    # Then the summary surfaces the single failure and the applied count
    assert summary.failed is True
    assert summary.failed_count == 1
    assert summary.applied_count == 1
    assert tuple(f.message for f in summary.failures) == ("bang",)


def test_execution_summary_defaults_to_an_empty_unattempted_run():
    # Given no execution happened (e.g. an earlier phase short-circuited)
    summary = ExecutionSummary()

    # Then it is an empty, non-failing summary
    assert summary.results == ()
    assert summary.failed is False
    assert summary.failed_count == 0
    assert summary.applied_count == 0


def test_execution_outcome_variants_carry_the_right_payload():
    # Given the two execution outcomes
    succeeded = ExecutionSucceeded(statement_index=0, statement="SQL")
    failed = ExecutionFailure(
        statement_index=1,
        exception_type="E",
        message="m",
        statement="SQL",
    )

    # Then success and failure carry only the fields appropriate to their arm
    assert failed.exception_type == "E"
    assert not hasattr(succeeded, "exception_type")


# ---------- ExecutionSummary chronology ----------


@pytest.mark.parametrize(
    "results",
    [
        (_ok_exec(1),),
        (_ok_exec(0), _ok_exec(2)),
    ],
)
def test_execution_summary_rejects_non_contiguous_indexes(results):
    with pytest.raises(ValueError, match="indexes must be contiguous"):
        ExecutionSummary(results)


@pytest.mark.parametrize(
    "results",
    [
        (_failed_exec(0), _ok_exec(1)),
        (_failed_exec(0), _failed_exec(1)),
    ],
)
def test_execution_summary_rejects_results_after_a_failure(results):
    with pytest.raises(ValueError, match="failure must be the final result"):
        ExecutionSummary(results)
