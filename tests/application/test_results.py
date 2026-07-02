from datetime import datetime

from hypothesis import given, strategies as st

from delta_engine.application.results import (
    ExecutionFailed,
    ExecutionFailure,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
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
        failure=ExecutionFailure(
            action_index=1, exception_type="E", message="m", statement_preview="SQL"
        ),
    )

    # Then a failure is only representable on the failed variant
    assert failed.failure.exception_type == "E"
    assert not hasattr(succeeded, "failure")


def test_table_status_success_when_all_actions_succeed():
    # Given successful read, no pre-execution failures, and only successful actions
    read = TablePresent(table=_an_observed_table())
    execution = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

    # When aggregating
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "schema", "tbl"),
        read=read,
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
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = TableRunReport(
        qualified_name=QualifiedName("cat", "s", "b"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_failed_exec(0),)),
        failures=(
            ExecutionFailure(
                action_index=0,
                exception_type="ValueError",
                message="boom",
                statement_preview="ALTER TABLE ...",
            ),
        ),
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


def test_sync_report_failures_by_table_maps_only_failed_tables():
    # Given one failed and one successful table
    ok_name = QualifiedName("cat", "s", "x")
    failed_name = QualifiedName("cat", "s", "y")
    t_ok = TableRunReport(
        qualified_name=ok_name,
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = TableRunReport(
        qualified_name=failed_name,
        read=TableAbsent(),
        failures=(ValidationFailure("R", "v"),),
        execution=ExecutionSummary(),
    )

    # When
    sr = SyncReport(started_at=_t0(), ended_at=_t1(), table_reports=(t_ok, t_bad))

    # Then only the failed table appears, keyed by its QualifiedName, with its failures
    mapping = sr.failures_by_table
    assert list(mapping.keys()) == [failed_name]
    assert all(
        isinstance(f, ValidationFailure | ReadFailure | ExecutionFailure | ForeignKeyFailure)
        for f in mapping[failed_name]
    )


def test_foreign_key_failure_renders_a_descriptive_line():
    # Given a foreign-key failure described by its content
    failure = ForeignKeyFailure(
        table=QualifiedName("cat", "sch", "orders"),
        local_columns=("customer_id",),
        references="cat.sch.customers",
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
    )

    # When rendering its lines
    lines = failure.format_lines()

    # Then the message identifies the FK by its local columns and referenced table
    assert lines == (
        "Foreign key (customer_id) → cat.sch.customers on cat.sch.orders was not applied:"
        " it references a table that is not registered.",
    )


def test_table_run_report_status_is_foreign_key_failed_when_fk_failure_present():
    # Given a table that read cleanly but has an FK failure in failures
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "orders"),
                local_columns=("customer_id",),
                references="cat.sch.customers",
                reason=ForeignKeyFailureReason.CYCLE,
            ),
        ),
    )

    # Then its status reflects the FK failure and it counts as a failure
    assert report.status is TableRunStatus.FOREIGN_KEY_FAILED
    assert report.has_failures is True
    assert report.failures[0].format_lines()[0].startswith("Foreign key")


def test_table_run_report_status_is_validation_failed_when_only_validation_failure_present():
    # Given a table that read cleanly but has a validation failure and no FK failure
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "sch", "tbl"),
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ValidationFailure(rule_name="NonNullableColumnAdd", message="cannot add NOT NULL"),
        ),
    )

    # Then its status is VALIDATION_FAILED (no FK failure takes priority)
    assert report.status is TableRunStatus.VALIDATION_FAILED
    assert report.has_failures is True


def test_table_run_report_status_is_validation_failed_when_both_fk_and_validation_present():
    # Given a table with both a validation failure and an FK failure
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ValidationFailure(rule_name="NonNullableColumnAdd", message="cannot add NOT NULL"),
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "orders"),
                local_columns=("customer_id",),
                references="cat.sch.customers",
                reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
            ),
        ),
    )

    # Then VALIDATION_FAILED wins: it is the earlier phase and the actionable root cause
    assert report.status is TableRunStatus.VALIDATION_FAILED
    assert len(report.failures) == 2


def test_table_run_report_with_no_pre_execution_failures_is_success():
    # Given a clean table with no pre-execution failures
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "sch", "ok"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_ok_exec(0),)),
    )

    # Then it is a success and carries no failures
    assert report.status is TableRunStatus.SUCCESS
    assert report.failures == ()


def test_foreign_key_failure_renders_not_a_key_reason():
    # Given a FK failure because the referenced columns are not the parent's PK
    failure = ForeignKeyFailure(
        table=QualifiedName("cat", "sch", "orders"),
        local_columns=("customer_id",),
        references="cat.sch.customers",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
    )

    # When rendered
    [line] = failure.format_lines()

    # Then the message names the local columns, referenced table, owning table, and reason
    assert "(customer_id)" in line
    assert "cat.sch.customers" in line
    assert "cat.sch.orders" in line
    assert "not the primary key" in line


def test_each_failure_kind_declares_its_producing_phase():
    # Given the four failure kinds
    # Then each declares the phase that produced it, ordered read < validation < fk < execution
    from delta_engine.application.results import FailurePhase

    assert ReadFailure("E", "m").phase is FailurePhase.READ
    assert ValidationFailure("R", "m").phase is FailurePhase.VALIDATION
    assert (
        ForeignKeyFailure(
            table=QualifiedName("c", "s", "t"),
            local_columns=("x",),
            references="c.s.o",
            reason=ForeignKeyFailureReason.CYCLE,
        ).phase
        is FailurePhase.FOREIGN_KEY
    )
    assert (
        ExecutionFailure(
            action_index=0, exception_type="E", message="m", statement_preview="SQL"
        ).phase
        is FailurePhase.EXECUTION
    )
    assert (
        FailurePhase.READ
        < FailurePhase.VALIDATION
        < FailurePhase.FOREIGN_KEY
        < FailurePhase.EXECUTION
    )


def test_foreign_key_reason_detail_is_defined_for_every_member():
    # Given every FK failure reason
    # Then each renders a non-empty human-readable detail string from the enum itself
    for reason in ForeignKeyFailureReason:
        assert reason.detail
        assert isinstance(reason.detail, str)


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


def test_status_reflects_the_earliest_failing_phase():
    # Given a table with an execution failure only
    read = TablePresent(table=_an_observed_table())
    exec_only = TableRunReport(
        qualified_name=QualifiedName("cat", "s", "e"),
        read=read,
        execution=ExecutionSummary((_failed_exec(0),)),
        failures=(
            ExecutionFailure(
                action_index=0, exception_type="E", message="m", statement_preview="SQL"
            ),
        ),
    )
    # Then it is EXECUTION_FAILED
    assert exec_only.status is TableRunStatus.EXECUTION_FAILED

    # Given a read failure present in the stream, it dominates any later phase
    read_and_exec = TableRunReport(
        qualified_name=QualifiedName("cat", "s", "r"),
        read=ReadFailed(ReadFailure("IOError", "boom")),
        failures=(
            ReadFailure("IOError", "boom"),
            ExecutionFailure(
                action_index=0, exception_type="E", message="m", statement_preview="SQL"
            ),
        ),
    )
    # Then READ_FAILED wins (earliest phase)
    assert read_and_exec.status is TableRunStatus.READ_FAILED
