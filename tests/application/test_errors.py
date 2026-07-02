from datetime import UTC, datetime

from delta_engine.application.errors import SyncFailedError
from delta_engine.application.results import (
    CatalogState,
    ExecutionFailed,
    ExecutionFailure,
    ExecutionSummary,
    Failure,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailed,
    ReadFailure,
    SyncReport,
    TableAbsent,
    TableRunReport,
    ValidationFailure,
)
from delta_engine.domain.model import QualifiedName

_AT = datetime(2026, 1, 1, tzinfo=UTC)
_QN = QualifiedName("cat", "sch", "tbl")


def _table_report(
    *,
    read: CatalogState,
    pre_execution_failures: tuple[Failure, ...] = (),
    execution: ExecutionSummary | None = None,
) -> TableRunReport:
    return TableRunReport(
        qualified_name=_QN,
        read=read,
        pre_execution_failures=pre_execution_failures,
        execution=execution,
    )


def _message_for(table_report: TableRunReport) -> str:
    """Return the SyncFailedError message produced for a run with this one failed table."""
    report = SyncReport(started_at=_AT, ended_at=_AT, table_reports=(table_report,))
    return str(SyncFailedError(report))


def test_message_headline_counts_failed_tables():
    # Given a run with a single failed table
    report = _table_report(read=ReadFailed(ReadFailure("AnalysisException", "table not found")))

    # When building the error message
    message = _message_for(report)

    # Then the headline reports the failed/total tally
    assert message.startswith("Sync failed: 1/1 tables failed")


def test_message_renders_read_failure_detail():
    # Given a table whose read phase failed
    report = _table_report(read=ReadFailed(ReadFailure("AnalysisException", "table not found")))

    # When building the error message
    message = _message_for(report)

    # Then the table headline and the read error line are present
    assert "❌ cat.sch.tbl [READ_FAILED]" in message
    assert "Read error: AnalysisException - table not found" in message


def test_message_renders_validation_failure_detail():
    # Given a table whose validation phase failed
    report = _table_report(
        read=TableAbsent(),
        pre_execution_failures=(
            ValidationFailure("DisallowPartitioningChange", "cannot repartition"),
        ),
    )

    # When building the error message
    message = _message_for(report)

    # Then the validation failure line is present
    assert "❌ cat.sch.tbl [VALIDATION_FAILED]" in message
    assert "Validation failed: DisallowPartitioningChange - cannot repartition" in message


def test_message_renders_every_validation_failure_when_a_table_breaks_several_rules():
    # Given a table whose plan tripped two rules at once
    report = _table_report(
        read=TableAbsent(),
        pre_execution_failures=(
            ValidationFailure("NonNullableColumnAdd", "cannot add NOT NULL column 'age'"),
            ValidationFailure("DisallowPartitioningChange", "cannot repartition"),
        ),
    )

    # When building the error message
    message = _message_for(report)

    # Then BOTH failures are surfaced together, not just the first
    assert "Validation failed: NonNullableColumnAdd - cannot add NOT NULL column 'age'" in message
    assert "Validation failed: DisallowPartitioningChange - cannot repartition" in message


def test_message_renders_execution_failure_detail_with_sql_preview():
    # Given a table whose execution phase failed on one action
    failed_result = ExecutionFailed(
        action="AddColumn",
        failure=ExecutionFailure(
            action_index=2,
            exception_type="SparkException",
            message="boom",
            statement_preview="ALTER TABLE cat.sch.tbl ADD COLUMN x INT",
        ),
    )
    report = _table_report(read=TableAbsent(), execution=ExecutionSummary((failed_result,)))

    # When building the error message
    message = _message_for(report)

    # Then both the failure line and the SQL preview are present
    assert "❌ cat.sch.tbl [EXECUTION_FAILED]" in message
    assert "Execution failed at action 2: SparkException - boom" in message
    assert "ALTER TABLE cat.sch.tbl ADD COLUMN x INT" in message


def test_message_renders_fk_failure_detail():
    # Given a table with an FK failure described by its content
    report = _table_report(
        read=TableAbsent(),
        pre_execution_failures=(
            ForeignKeyFailure(
                table=_QN,
                local_columns=("ref_id",),
                references="cat.sch.other",
                reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
            ),
        ),
    )

    # When building the error message
    message = _message_for(report)

    # Then the FK failure line is present with content-based description
    assert "❌ cat.sch.tbl [FOREIGN_KEY_FAILED]" in message
    assert "(ref_id)" in message
    assert "not registered" in message
