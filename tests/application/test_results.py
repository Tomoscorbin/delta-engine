from datetime import datetime

import pytest

from delta_engine.application.results import (
    ActionStatus,
    ExecutionFailure,
    ExecutionResult,
    ReadFailed,
    ReadFailure,
    ReadSucceeded,
    SyncReport,
    TableRunReport,
    TableRunStatus,
    ValidationFailure,
    ValidationResult,
)

# ---------- test fakes / builders


class _FakeObservedTable:
    def __init__(self, name="dummy", partitioned_by=()):
        self.fully_qualified_name = name  # not used here, kept for parity
        self.partitioned_by = partitioned_by


def _t0():
    return datetime(2025, 10, 2, 12, 0, 0)


def _t1():
    return datetime(2025, 10, 2, 12, 5, 0)


def _ok_exec(idx=0, action="AddColumn", preview="ALTER TABLE ..."):
    return ExecutionResult(
        action=action,
        action_index=idx,
        status=ActionStatus.OK,
        statement_preview=preview,
        failure=None,
    )


def _noop_exec(idx=0, action="SetProperty", preview="-- NOOP"):
    return ExecutionResult(
        action=action,
        action_index=idx,
        status=ActionStatus.NOOP,
        statement_preview=preview,
        failure=None,
    )


def _failed_exec(
    idx=0, action="AddColumn", preview="ALTER TABLE ...", exc="ValueError", msg="boom"
):
    return ExecutionResult(
        action=action,
        action_index=idx,
        status=ActionStatus.FAILED,
        statement_preview=preview,
        failure=ExecutionFailure(action_index=idx, exception_type=exc, message=msg),
    )


# ---------- Tests


def test_read_succeeded_present_holds_the_observed_table():
    # Given a table that was read and exists
    observed = _FakeObservedTable()

    # When recording a successful read
    result = ReadSucceeded(observed=observed)

    # Then it carries the observed table and is not a failure
    assert result.observed is observed
    assert not isinstance(result, ReadFailed)


def test_read_succeeded_absent_carries_no_observed_table():
    # Given a table that was read but does not exist

    # When recording a successful read with no table
    result = ReadSucceeded(observed=None)

    # Then absence is represented by a null observed table, still not a failure
    assert result.observed is None
    assert not isinstance(result, ReadFailed)


def test_read_failed_carries_the_failure():
    # Given a read that raised
    failure = ReadFailure(exception_type="RuntimeError", message="catalog unreachable")

    # When recording a failed read
    result = ReadFailed(failure=failure)

    # Then it records the failure
    assert result.failure is failure


def test_validation_result_failed_property_reflects_presence_of_failures():
    # Given a result with failures
    vf = ValidationFailure(rule_name="SomeRule", message="nope")

    # When checking .failed
    failed_result = ValidationResult(failures=(vf,))
    ok_result = ValidationResult()

    # Then it reports correctly
    assert failed_result.failed is True
    assert ok_result.failed is False


def test_execution_result_enforces_failure_presence_for_failed_status():
    # Given a FAILED status without a failure
    # When/Then it raises a ValueError
    with pytest.raises(ValueError):
        ExecutionResult(
            action="DoThing",
            action_index=0,
            status=ActionStatus.FAILED,
            statement_preview="SQL",
            failure=None,
        )


def test_execution_result_forbids_failure_on_non_failed_status():
    # Given an OK status with an attached failure
    # When/Then it raises a ValueError
    with pytest.raises(ValueError):
        ExecutionResult(
            action="DoThing",
            action_index=0,
            status=ActionStatus.OK,
            statement_preview="SQL",
            failure=ExecutionFailure(action_index=0, exception_type="E", message="m"),
        )


def test_table_status_success_when_all_phases_ok():
    # Given successful read, no validation failures, and OK/NOOP actions only
    read = ReadSucceeded(observed=_FakeObservedTable())
    validation = ValidationResult()
    execution_results = (_ok_exec(0), _noop_exec(1))

    # When aggregating
    report = TableRunReport(
        fully_qualified_name="cat.schema.tbl",
        started_at=_t0(),
        ended_at=_t1(),
        read=read,
        validation=validation,
        execution_results=execution_results,
    )

    # Then everything is SUCCESS and has_failures is False
    assert report.status is TableRunStatus.SUCCESS
    assert report.has_failures is False
    assert report.action_failures == ()


def test_sync_report_any_failures_true_if_any_table_has_failures():
    # Given two tables: one success, one with execution failure
    t_ok = TableRunReport(
        fully_qualified_name="a",
        started_at=_t0(),
        ended_at=_t1(),
        read=ReadSucceeded(observed=_FakeObservedTable()),
        validation=ValidationResult(),
        execution_results=(_ok_exec(0),),
    )
    t_bad = TableRunReport(
        fully_qualified_name="b",
        started_at=_t0(),
        ended_at=_t1(),
        read=ReadSucceeded(observed=_FakeObservedTable()),
        validation=ValidationResult(),
        execution_results=(_failed_exec(0),),
    )

    # When aggregating the sync
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(t_ok, t_bad))

    # Then any_failures is True
    assert sr.any_failures is True


def test_sync_report_failures_by_table_maps_only_failed_tables():
    # Given one failed and one successful table
    t_ok = TableRunReport(
        fully_qualified_name="x",
        started_at=_t0(),
        ended_at=_t1(),
        read=ReadSucceeded(observed=_FakeObservedTable()),
        validation=ValidationResult(),
        execution_results=(_ok_exec(0),),
    )
    t_bad = TableRunReport(
        fully_qualified_name="y",
        started_at=_t0(),
        ended_at=_t1(),
        read=ReadSucceeded(observed=_FakeObservedTable()),
        validation=ValidationResult(failures=(ValidationFailure("R", "v"),)),
        execution_results=(),
    )

    # When
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(t_ok, t_bad))

    # Then only the failed table appears, with its failures
    mapping = sr.failures_by_table
    assert list(mapping.keys()) == ["y"]
    assert all(
        isinstance(f, ValidationFailure | ReadFailure | ExecutionFailure) for f in mapping["y"]
    )
