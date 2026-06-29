from datetime import datetime

from hypothesis import given, strategies as st

from delta_engine.application.results import (
    ExecutionFailed,
    ExecutionFailure,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
    ForeignKeyValidationReport,
    ReadFailed,
    ReadFailure,
    SkipReason,
    SkippedForeignKey,
    SyncReport,
    TableAbsent,
    TablePresent,
    TableRunReport,
    TableRunStatus,
    ValidationFailure,
    ValidationResult,
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


def test_read_failure_formats_itself_as_a_display_line():
    # Given a read failure
    failure = ReadFailure(exception_type="AnalysisException", message="table not found")

    # Then it renders its own one-line description
    assert failure.format_lines() == ("Read error: AnalysisException - table not found",)


def test_validation_failure_formats_itself_as_a_display_line():
    # Given a validation failure
    failure = ValidationFailure(
        rule_name="DisallowPartitioningChange", message="cannot repartition"
    )

    # Then it renders its own one-line description
    assert failure.format_lines() == (
        "Validation failed: DisallowPartitioningChange - cannot repartition",
    )


def test_execution_failure_formats_itself_as_two_lines_including_sql_preview():
    # Given an execution failure with a SQL preview
    failure = ExecutionFailure(
        action_index=2,
        exception_type="SparkException",
        message="boom",
        statement_preview="ALTER TABLE t ADD COLUMN x INT",
    )

    # Then it renders the error line and the SQL preview together
    lines = failure.format_lines()
    assert lines[0] == "Execution failed at action 2: SparkException - boom"
    assert "ALTER TABLE t ADD COLUMN x INT" in lines[1]


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
        failure=ExecutionFailure(
            action_index=1, exception_type="E", message="m", statement_preview="SQL"
        ),
    )

    # Then a failure is only representable on the failed variant
    assert failed.failure.exception_type == "E"
    assert not hasattr(succeeded, "failure")


def test_table_status_success_when_all_actions_succeed():
    # Given successful read, no validation failures, and only successful actions
    read = TablePresent(table=_an_observed_table())
    validation = ValidationResult()
    execution = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

    # When aggregating
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "schema", "tbl"),
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
        read=TablePresent(table=_an_observed_table()),
        validation=ValidationResult(),
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = TableRunReport(
        qualified_name=QualifiedName("cat", "s", "b"),
        read=TablePresent(table=_an_observed_table()),
        validation=ValidationResult(),
        execution=ExecutionSummary((_failed_exec(0),)),
    )

    # When aggregating the sync
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(t_ok, t_bad))

    # Then any_failures is True
    assert sr.any_failures is True


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
        action_index=st.integers(min_value=0, max_value=100),
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


def test_foreign_key_validation_report_defaults_to_no_skipped():
    # Given / When
    report = ForeignKeyValidationReport()

    # Then
    assert report.skipped == ()
    assert report.has_skipped is False


def test_foreign_key_validation_report_has_skipped_when_skipped_present():
    # Given
    skipped = SkippedForeignKey(
        table=QualifiedName("cat", "sch", "orders"),
        constraint_name="orders_customer_id_fk",
        reason=SkipReason.UNRESOLVABLE_REFERENCE,
    )

    # When
    report = ForeignKeyValidationReport(skipped=(skipped,))

    # Then
    assert report.has_skipped is True
    assert len(report.skipped) == 1


def test_sync_report_carries_fk_validation_report():
    # Given a sync report with no table reports
    # When
    report = SyncReport(
        started_at=_t0(),
        ended_at=_t1(),
        table_reports=(),
    )

    # Then it carries a default empty FK validation report
    assert isinstance(report.foreign_key_validation, ForeignKeyValidationReport)
    assert report.foreign_key_validation.has_skipped is False


def test_sync_report_failures_by_table_maps_only_failed_tables():
    # Given one failed and one successful table
    ok_name = QualifiedName("cat", "s", "x")
    failed_name = QualifiedName("cat", "s", "y")
    t_ok = TableRunReport(
        qualified_name=ok_name,
        read=TablePresent(table=_an_observed_table()),
        validation=ValidationResult(),
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = TableRunReport(
        qualified_name=failed_name,
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
