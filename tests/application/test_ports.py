from hypothesis import given, strategies as st

from delta_engine.application.failures import ExecutionFailure, ReadFailure
from delta_engine.application.ports import (
    ExecutionFailed,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
    ReadFailed,
    TableAbsent,
    TablePresent,
)
from delta_engine.domain.model import Column, Integer, ObservedTable, QualifiedName

# ---------- test builders


def _an_observed_table(partitioned_by=()):
    """Build a real ObservedTable, so reports are exercised against the domain type."""
    return ObservedTable(
        qualified_name=QualifiedName("cat", "schema", "observed"),
        columns=(Column("id", Integer()),),
        partitioned_by=partitioned_by,
    )


def _ok_exec(idx=0, action="AddColumn", preview="ALTER TABLE ..."):
    return ExecutionSucceeded(action=action, action_index=idx, statement_preview=preview)


def _failed_exec(
    idx=0, action="AddColumn", preview="ALTER TABLE ...", exc="ValueError", msg="boom"
):
    return ExecutionFailed(
        action=action,
        failure=ExecutionFailure(
            action_index=idx, exception_type=exc, message=msg, statement_preview=preview
        ),
    )


# ---------- Tests


def test_present_state_holds_the_observed_table():
    # Given a table that was read and exists
    observed = _an_observed_table()

    # When recording its catalog state
    state = TablePresent(table=observed)

    # Then it carries the observed table and is not a failure
    assert state.table is observed
    assert not isinstance(state, ReadFailed)


def test_absent_state_is_distinct_from_a_failure():
    # Given a table that was read but does not exist

    # When recording its catalog state
    state = TableAbsent()

    # Then absence is its own state, not a failure
    assert not isinstance(state, ReadFailed)


def test_read_failed_carries_the_failure():
    # Given a read that raised
    failure = ReadFailure(exception_type="RuntimeError", message="catalog unreachable")

    # When recording a failed read
    result = ReadFailed(failure=failure)

    # Then it records the failure
    assert result.failure is failure


def test_execution_summary_reports_no_failure_when_every_action_succeeds():
    # Given a plan whose actions all executed
    summary = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

    # Then the summary reports success with no failures
    assert summary.failed is False
    assert summary.failures == ()
    assert summary.failed_count == 0


def test_execution_summary_exposes_the_failures_among_mixed_results():
    # Given a plan that ran two actions, the second of which failed
    summary = ExecutionSummary((_ok_exec(0), _failed_exec(1, msg="bang")))

    # Then the summary surfaces the single failure and its count
    assert summary.failed is True
    assert summary.failed_count == 1
    assert tuple(f.message for f in summary.failures) == ("bang",)


def test_execution_summary_defaults_to_an_empty_unattempted_run():
    # Given no execution happened (e.g. an earlier phase short-circuited)
    summary = ExecutionSummary()

    # Then it is an empty, non-failing summary
    assert summary.results == ()
    assert summary.failed is False
    assert summary.failed_count == 0


def test_execution_outcome_variants_carry_the_right_payload():
    # Given the two execution outcomes
    succeeded = ExecutionSucceeded(action="AddColumn", action_index=0, statement_preview="SQL")
    failed = ExecutionFailed(
        action="AddColumn",
        failure=ExecutionFailure(
            action_index=1, exception_type="E", message="m", statement_preview="SQL"
        ),
    )

    # Then a failure is only representable on the failed variant
    assert failed.failure.exception_type == "E"
    assert not hasattr(succeeded, "failure")


def test_execution_failed_carries_index_only_on_its_failure_detail():
    # Given a failed action
    failed = ExecutionFailed(
        action="AddColumn",
        failure=ExecutionFailure(
            action_index=3, exception_type="E", message="m", statement_preview="SQL"
        ),
    )

    # Then the index lives on the failure detail, not duplicated on the carrier
    assert failed.failure.action_index == 3
    assert not hasattr(failed, "action_index")


# ---------- property: ExecutionSummary internal consistency ----------


_EXECUTION_RESULT = st.one_of(
    st.builds(
        ExecutionSucceeded,
        action=st.just("AddColumn"),
        action_index=st.integers(min_value=0, max_value=100),
        statement_preview=st.just("ALTER TABLE ..."),
    ),
    st.builds(
        ExecutionFailed,
        action=st.just("AddColumn"),
        failure=st.builds(
            ExecutionFailure,
            action_index=st.integers(min_value=0, max_value=100),
            exception_type=st.just("SparkException"),
            message=st.text(max_size=40),
            statement_preview=st.just("ALTER TABLE ..."),
        ),
    ),
)


@given(st.lists(_EXECUTION_RESULT, max_size=10))
def test_execution_summary_failed_count_and_failures_are_mutually_consistent(
    results: list[ExecutionResult],
) -> None:
    # Given: any mix of succeeded and failed execution results
    summary = ExecutionSummary(tuple(results))

    # Then: failed, failures, and failed_count all agree
    assert summary.failed == (summary.failed_count > 0)
    assert summary.failed_count == len(summary.failures)
    assert all(isinstance(f, ExecutionFailure) for f in summary.failures)
