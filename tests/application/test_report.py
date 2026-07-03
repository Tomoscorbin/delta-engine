from datetime import datetime

from delta_engine.application.failures import (
    ExecutionFailure,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.ports import (
    ExecutionFailed,
    ExecutionSucceeded,
    ExecutionSummary,
    ReadFailed,
    TableAbsent,
    TablePresent,
)
from delta_engine.application.report import (
    SyncReport,
    TableRunReport,
    TableRunStatus,
)
from delta_engine.domain.model import Column, DesiredTable, Integer, ObservedTable, QualifiedName
from delta_engine.domain.plan.actions import ActionPlan

# ---------- test builders


def _an_observed_table(partitioned_by=()):
    """Build a real ObservedTable, so reports are exercised against the domain type."""
    return ObservedTable(
        qualified_name=QualifiedName("cat", "schema", "observed"),
        columns=(Column("id", Integer()),),
        partitioned_by=partitioned_by,
    )


def _a_desired_table(name="observed"):
    """Build a minimal real DesiredTable for pipeline-record construction."""
    return DesiredTable(
        qualified_name=QualifiedName("cat", "schema", name),
        columns=(Column("id", Integer()),),
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


def test_table_status_success_when_all_actions_succeed():
    # Given successful read, no pre-execution failures, and only successful actions
    read = TablePresent(table=_an_observed_table())
    execution = ExecutionSummary((_ok_exec(0), _ok_exec(1)))

    # When aggregating
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "schema", "tbl"),
        desired=_a_desired_table("tbl"),
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
        desired=_a_desired_table("a"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = TableRunReport(
        qualified_name=QualifiedName("cat", "s", "b"),
        desired=_a_desired_table("b"),
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


def test_sync_report_failures_by_table_maps_only_failed_tables():
    # Given one failed and one successful table
    ok_name = QualifiedName("cat", "s", "x")
    failed_name = QualifiedName("cat", "s", "y")
    t_ok = TableRunReport(
        qualified_name=ok_name,
        desired=_a_desired_table("x"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_ok_exec(0),)),
    )
    t_bad = TableRunReport(
        qualified_name=failed_name,
        desired=_a_desired_table("y"),
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


def test_table_run_report_status_is_foreign_key_failed_when_fk_failure_present():
    # Given a table that read cleanly but has an FK failure in failures
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "orders"),
                local_columns=("customer_id",),
                references=QualifiedName("cat", "sch", "customers"),
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
        desired=_a_desired_table("tbl"),
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
        desired=_a_desired_table("orders"),
        read=TablePresent(table=_an_observed_table()),
        failures=(
            ValidationFailure(rule_name="NonNullableColumnAdd", message="cannot add NOT NULL"),
            ForeignKeyFailure(
                table=QualifiedName("cat", "sch", "orders"),
                local_columns=("customer_id",),
                references=QualifiedName("cat", "sch", "customers"),
                reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
            ),
        ),
    )

    # Then VALIDATION_FAILED wins: it is the earlier phase and the actionable root cause
    assert report.status is TableRunStatus.VALIDATION_FAILED
    assert len(report.failures) == 2


def test_table_run_report_with_no_failures_is_success():
    # Given a clean table with no failures
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "sch", "ok"),
        desired=_a_desired_table("ok"),
        read=TablePresent(table=_an_observed_table()),
        execution=ExecutionSummary((_ok_exec(0),)),
    )

    # Then it is a success and carries no failures
    assert report.status is TableRunStatus.SUCCESS
    assert report.failures == ()


def test_status_reflects_the_earliest_failing_phase():
    # Given a table with an execution failure only
    read = TablePresent(table=_an_observed_table())
    exec_only = TableRunReport(
        qualified_name=QualifiedName("cat", "s", "e"),
        desired=_a_desired_table("e"),
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
        desired=_a_desired_table("r"),
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


def test_table_run_report_carries_its_desired_definition():
    # Given a freshly-born run: read set, other phase fields at defaults
    desired = _a_desired_table("customers")
    report = TableRunReport(
        qualified_name=QualifiedName("cat", "schema", "customers"),
        desired=desired,
        read=TablePresent(table=_an_observed_table()),
    )

    # Then it carries the desired definition and is a clean success so far
    assert report.desired is desired
    assert report.status is TableRunStatus.SUCCESS
    assert report.failures == ()
    assert report.plan == ActionPlan()
