from datetime import datetime, timedelta

import pytest

from delta_engine.application.results import (
    ActionStatus,
    ExecutionFailure,
    ExecutionResult,
    ReadFailure,
    ReadResult,
    SyncReport,
    TableRunReport,
    TableRunStatus,
    ValidationFailure,
    ValidationResult,
)
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from tests.factories import make_observed_table, make_qualified_name

_QN = make_qualified_name("dev", "silver", "orders")
_OBSERVED = make_observed_table(_QN, (Column("id", Integer()),))


def _qn() -> str:
    return "dev.silver.people"


def _now_pair() -> tuple[datetime, datetime]:
    start = datetime.now()
    end = start + timedelta(seconds=1)
    return start, end


def test_read_result_present_absent_failed_flags() -> None:
    present = ReadResult.create_present(_OBSERVED)
    absent = ReadResult.create_absent()
    failed = ReadResult.create_failed(ReadFailure(exception_type="IOError", message="boom"))

    assert present.observed is not None
    assert present.failure is None
    assert present.failed is False
    assert absent.observed is None
    assert absent.failure is None
    assert absent.failed is False
    assert failed.observed is None
    assert failed.failure is not None
    assert failed.failed is True


def test_validation_result_failed_boolean() -> None:
    ok = ValidationResult(failures=())
    bad = ValidationResult(failures=(ValidationFailure("RuleX", "nope"),))

    assert ok.failed is False
    assert bad.failed is True


def test_failed_result_must_carry_failure() -> None:
    with pytest.raises(ValueError):
        ExecutionResult(
            action="AddColumn(age INT)",
            action_index=1,
            status=ActionStatus.FAILED,
            statement_preview="ALTER TABLE ...",
            failure=None,  # not allowed
        )


def test_non_failed_result_must_not_carry_failure() -> None:
    with pytest.raises(ValueError):
        ExecutionResult(
            action="AddColumn(age INT)",
            action_index=1,
            status=ActionStatus.OK,
            statement_preview="ALTER TABLE ...",
            failure=ExecutionFailure(
                action_index=1, exception_type="X", message="Y"
            ),  # not allowed
        )


def test_status_precedence_read_overrides_others() -> None:
    start, end = _now_pair()
    rpt = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=start,
        ended_at=end,
        read=ReadResult(failure=ReadFailure("IOError", "cannot read")),
        validation=ValidationResult(failures=(ValidationFailure("R", "x"),)),
        execution_results=(
            ExecutionResult(
                action="AddColumn(age)",
                action_index=0,
                status=ActionStatus.FAILED,
                statement_preview="ALTER TABLE ...",
                failure=ExecutionFailure(0, "Ex", "boom"),
            ),
        ),
    )
    assert rpt.status == TableRunStatus.READ_FAILED


def test_status_precedence_validation_over_execution() -> None:
    start, end = _now_pair()
    rpt = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=start,
        ended_at=end,
        read=ReadResult.create_present(_OBSERVED),
        validation=ValidationResult(failures=(ValidationFailure("Rule", "bad"),)),
        execution_results=(
            ExecutionResult(
                action="AddColumn(age)",
                action_index=0,
                status=ActionStatus.FAILED,
                statement_preview="ALTER TABLE ...",
                failure=ExecutionFailure(0, "Ex", "boom"),
            ),
        ),
    )
    assert rpt.status == TableRunStatus.VALIDATION_FAILED


def test_status_execution_failed_when_any_action_failed_and_no_read_or_validation_failures() -> (
    None
):
    start, end = _now_pair()
    rpt = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=start,
        ended_at=end,
        read=ReadResult.create_absent(),  # not a failure
        validation=ValidationResult(failures=()),
        execution_results=(
            ExecutionResult(
                action="AddColumn(age)",
                action_index=0,
                status=ActionStatus.OK,
                statement_preview="ALTER TABLE ...",
            ),
            ExecutionResult(
                action="AddColumn(name)",
                action_index=1,
                status=ActionStatus.FAILED,
                statement_preview="ALTER TABLE ...",
                failure=ExecutionFailure(1, "ParseException", "bad SQL"),
            ),
        ),
    )
    assert rpt.status == TableRunStatus.EXECUTION_FAILED
    assert rpt.has_failures is True
    assert rpt._any_action_failed is True
    assert len(rpt.action_failures) == 1
    assert isinstance(rpt.action_failures[0], ExecutionFailure)


def test_status_success_when_no_failures_anywhere() -> None:
    start, end = _now_pair()
    rpt = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=start,
        ended_at=end,
        read=ReadResult.create_present(_OBSERVED),
        validation=ValidationResult(failures=()),
        execution_results=(
            ExecutionResult(
                action="CreateTable",
                action_index=0,
                status=ActionStatus.OK,
                statement_preview="CREATE TABLE ...",
            ),
        ),
    )
    assert rpt.status == TableRunStatus.SUCCESS
    assert rpt.has_failures is False
    assert rpt.action_failures == ()


def test_status_validation_result_is_empty_and_no_failures_means_success() -> None:
    start, end = _now_pair()
    rpt = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=start,
        ended_at=end,
        read=ReadResult.create_present(_OBSERVED),
        validation=ValidationResult(failures=()),
        execution_results=(
            ExecutionResult(
                action="CreateTable",
                action_index=0,
                status=ActionStatus.OK,
                statement_preview="CREATE TABLE ...",
            ),
        ),
    )
    assert rpt.status == TableRunStatus.SUCCESS
    assert rpt.has_failures is False


def test_status_validation_is_empty_but_action_failed_is_execution_failed() -> None:
    start, end = _now_pair()
    rpt = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=start,
        ended_at=end,
        read=ReadResult.create_present(_OBSERVED),
        validation=ValidationResult(failures=()),
        execution_results=(
            ExecutionResult(
                action="AddColumn(age)",
                action_index=0,
                status=ActionStatus.FAILED,
                statement_preview="ALTER TABLE ...",
                failure=ExecutionFailure(0, "Ex", "boom"),
            ),
        ),
    )
    assert rpt.status == TableRunStatus.EXECUTION_FAILED
    assert rpt.has_failures is True


def test_all_failures_contains_validation_and_execution_failures_but_not_duplicates() -> None:
    start, end = _now_pair()
    v1 = ValidationFailure("RuleX", "reason X")
    v2 = ValidationFailure("RuleY", "reason Y")
    f_exec = ExecutionFailure(1, "Ex", "boom")

    rpt = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=start,
        ended_at=end,
        read=ReadResult.create_present(_OBSERVED),
        validation=ValidationResult(failures=(v1, v2)),
        execution_results=(
            ExecutionResult(
                action="AddColumn(x)",
                action_index=1,
                status=ActionStatus.FAILED,
                statement_preview="ALTER ...",
                failure=f_exec,
            ),
        ),
    )

    failures = rpt.all_failures
    assert len(failures) == 3
    assert v1 in failures
    assert v2 in failures
    assert f_exec in failures


def test_all_failures_with_read_failure_only() -> None:
    start, end = _now_pair()
    rf = ReadFailure("IO", "cannot read")
    rpt = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=start,
        ended_at=end,
        read=ReadResult.create_failed(rf),
        validation=ValidationResult(failures=()),
        execution_results=(),
    )
    assert rpt.all_failures == (rf,)
    assert rpt.status == TableRunStatus.READ_FAILED
    assert rpt.has_failures is True


def test_failures_by_table_aggregates_validation_and_execution_failures() -> None:
    s1, e1 = _now_pair()
    s2, e2 = _now_pair()

    # Table A: validation failures
    t1 = TableRunReport(
        fully_qualified_name="dev.silver.orders",
        started_at=s1,
        ended_at=e1,
        read=ReadResult.create_present(_OBSERVED),
        validation=ValidationResult(failures=(ValidationFailure("Rule", "bad"),)),
        execution_results=(),
    )

    # Table B: execution failure
    exec_fail = ExecutionFailure(0, "ParseException", "bad SQL")
    t2 = TableRunReport(
        fully_qualified_name="dev.gold.payments",
        started_at=s2,
        ended_at=e2,
        read=ReadResult.create_absent(),
        validation=ValidationResult(failures=()),
        execution_results=(
            ExecutionResult(
                action="AddColumn(total)",
                action_index=0,
                status=ActionStatus.FAILED,
                statement_preview="ALTER ...",
                failure=exec_fail,
            ),
        ),
    )

    rpt = SyncReport(
        started_at=min(s1, s2),
        ended_at=max(e1, e2),
        table_reports=(t1, t2),
    )

    assert rpt.any_failures is True

    fb = rpt.failures_by_table
    assert set(fb.keys()) == {"dev.silver.orders", "dev.gold.payments"}
    # Types present as expected
    assert isinstance(fb["dev.silver.orders"][0], ValidationFailure)
    assert exec_fail in fb["dev.gold.payments"]

    # Iteration order preserved
    assert list(iter(rpt)) == [t1, t2]


def test_failures_by_table_empty_when_all_success() -> None:
    s, e = _now_pair()
    t = TableRunReport(
        fully_qualified_name=_qn(),
        started_at=s,
        ended_at=e,
        read=ReadResult.create_present(_OBSERVED),
        validation=ValidationResult(failures=()),
        execution_results=(
            ExecutionResult(
                action="CreateTable",
                action_index=0,
                status=ActionStatus.OK,
                statement_preview="CREATE TABLE ...",
            ),
        ),
    )

    rpt = SyncReport(started_at=s, ended_at=e, table_reports=(t,))
    assert rpt.any_failures is False
    assert rpt.failures_by_table == {}
