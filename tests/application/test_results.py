from datetime import datetime

from delta_engine.application.results import (
    ExecutionFailed,
    ExecutionFailure,
    ExecutionSucceeded,
    ExecutionSummary,
    ReadFailed,
    ReadFailure,
    SyncReport,
    TableAbsent,
    TablePresent,
    TableRunReport,
    TableRunStatus,
    ValidationFailure,
    ValidationResult,
)
from delta_engine.domain.model import QualifiedName

# ---------- test fakes / builders


class _FakeObservedTable:
    def __init__(self, partitioned_by=()):
        self.partitioned_by = partitioned_by


def _t0():
    return datetime(2025, 10, 2, 12, 0, 0)


def _t1():
    return datetime(2025, 10, 2, 12, 5, 0)


def _ok_exec(idx=0, action="AddColumn", preview="ALTER TABLE ..."):
    return ExecutionSucceeded(action=action, action_index=idx, statement_preview=preview)


def _failed_exec(
    idx=0, action="AddColumn", preview="ALTER TABLE ...", exc="ValueError", msg="boom"
):
    return ExecutionFailed(
        action=action,
        action_index=idx,
        statement_preview=preview,
        failure=ExecutionFailure(action_index=idx, exception_type=exc, message=msg),
    )


# ---------- Tests


def test_present_state_holds_the_observed_table():
    # Given a table that was read and exists
    observed = _FakeObservedTable()

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


def test_read_failure_formats_itself_as_a_display_line():
    # Given a read failure
    failure = ReadFailure(exception_type="AnalysisException", message="table not found")

    # Then it renders its own one-line description
    assert failure.format_line() == "Read error: AnalysisException - table not found"


def test_validation_failure_formats_itself_as_a_display_line():
    # Given a validation failure
    failure = ValidationFailure(
        rule_name="DisallowPartitioningChange", message="cannot repartition"
    )

    # Then it renders its own one-line description
    assert (
        failure.format_line()
        == "Validation failed: DisallowPartitioningChange - cannot repartition"
    )


def test_execution_failure_formats_itself_as_a_display_line():
    # Given an execution failure at a known action index
    failure = ExecutionFailure(action_index=2, exception_type="SparkException", message="boom")

    # Then it renders its own one-line description including the action index
    assert failure.format_line() == "Execution failed at action 2: SparkException - boom"


def test_validation_result_failed_property_reflects_presence_of_failures():
    # Given a result with failures
    vf = ValidationFailure(rule_name="SomeRule", message="nope")

    # When checking .failed
    failed_result = ValidationResult(failures=(vf,))
    ok_result = ValidationResult()

    # Then it reports correctly
    assert failed_result.failed is True
    assert ok_result.failed is False


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
        action_index=1,
        statement_preview="SQL",
        failure=ExecutionFailure(action_index=1, exception_type="E", message="m"),
    )

    # Then a failure is only representable on the failed variant
    assert failed.failure.exception_type == "E"
    assert not hasattr(succeeded, "failure")


def test_table_status_success_when_all_actions_succeed():
    # Given successful read, no validation failures, and only successful actions
    read = TablePresent(table=_FakeObservedTable())
    validation = ValidationResult()
    execution = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

    # When aggregating
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "schema", "tbl"),
        started_at=_t0(),
        ended_at=_t1(),
        read=read,
        validation=validation,
        execution=execution,
    )

    # Then everything is SUCCESS and has_failures is False
    assert report.status is TableRunStatus.SUCCESS
    assert report.has_failures is False
    assert report.execution.failures == ()


def test_sync_report_any_failures_true_if_any_table_has_failures():
    # Given two tables: one success, one with execution failure
    t_ok = TableRunReport(
        qualified_name=QualifiedName("cat", "s", "a"),
        started_at=_t0(),
        ended_at=_t1(),
        read=TablePresent(table=_FakeObservedTable()),
        validation=ValidationResult(),
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = TableRunReport(
        qualified_name=QualifiedName("cat", "s", "b"),
        started_at=_t0(),
        ended_at=_t1(),
        read=TablePresent(table=_FakeObservedTable()),
        validation=ValidationResult(),
        execution=ExecutionSummary((_failed_exec(0),)),
    )

    # When aggregating the sync
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(t_ok, t_bad))

    # Then any_failures is True
    assert sr.any_failures is True


def test_sync_report_failures_by_table_maps_only_failed_tables():
    # Given one failed and one successful table
    ok_name = QualifiedName("cat", "s", "x")
    failed_name = QualifiedName("cat", "s", "y")
    t_ok = TableRunReport(
        qualified_name=ok_name,
        started_at=_t0(),
        ended_at=_t1(),
        read=TablePresent(table=_FakeObservedTable()),
        validation=ValidationResult(),
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = TableRunReport(
        qualified_name=failed_name,
        started_at=_t0(),
        ended_at=_t1(),
        read=TableAbsent(),
        validation=ValidationResult(failures=(ValidationFailure("R", "v"),)),
        execution=ExecutionSummary(),
    )

    # When
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(t_ok, t_bad))

    # Then only the failed table appears, keyed by its QualifiedName, with its failures
    mapping = sr.failures_by_table
    assert list(mapping.keys()) == [failed_name]
    assert all(
        isinstance(f, ValidationFailure | ReadFailure | ExecutionFailure)
        for f in mapping[failed_name]
    )
