from datetime import UTC, datetime

from delta_engine.application.errors import SyncFailedError
from delta_engine.application.results import (
    CatalogState,
    ExecutionFailed,
    ExecutionFailure,
    ExecutionSummary,
    ReadFailed,
    ReadFailure,
    SyncReport,
    TableAbsent,
    TableRunReport,
    ValidationFailure,
    ValidationResult,
)
from delta_engine.domain.model import QualifiedName

_AT = datetime(2026, 1, 1, tzinfo=UTC)
_NO_VALIDATION_FAILURES = ValidationResult()
_NO_EXECUTION = ExecutionSummary()


def _table_report(
    *,
    read: CatalogState,
    validation: ValidationResult = _NO_VALIDATION_FAILURES,
    execution: ExecutionSummary = _NO_EXECUTION,
) -> TableRunReport:
    return TableRunReport(
        qualified_name=QualifiedName("cat", "sch", "tbl"),
        started_at=_AT,
        ended_at=_AT,
        read=read,
        validation=validation,
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
        validation=ValidationResult(
            failures=(ValidationFailure("DisallowPartitioningChange", "cannot repartition"),)
        ),
    )

    # When building the error message
    message = _message_for(report)

    # Then the validation failure line is present
    assert "❌ cat.sch.tbl [VALIDATION_FAILED]" in message
    assert "Validation failed: DisallowPartitioningChange - cannot repartition" in message


def test_message_renders_execution_failure_detail_with_sql_preview():
    # Given a table whose execution phase failed on one action
    failed_result = ExecutionFailed(
        action="AddColumn",
        action_index=2,
        statement_preview="ALTER TABLE cat.sch.tbl ADD COLUMN x INT",
        failure=ExecutionFailure(action_index=2, exception_type="SparkException", message="boom"),
    )
    report = _table_report(read=TableAbsent(), execution=ExecutionSummary((failed_result,)))

    # When building the error message
    message = _message_for(report)

    # Then both the failure line and the SQL preview are present
    assert "❌ cat.sch.tbl [EXECUTION_FAILED]" in message
    assert "Execution failed at action 2: SparkException - boom" in message
    assert "Failed SQL preview (action 2):" in message
    assert "ALTER TABLE cat.sch.tbl ADD COLUMN x INT" in message
