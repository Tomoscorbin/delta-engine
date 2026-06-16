from datetime import UTC, datetime

from delta_engine.application.format_report import format_failure_detail
from delta_engine.application.results import (
    ActionStatus,
    ExecutionFailure,
    ExecutionResult,
    ReadFailed,
    ReadFailure,
    ReadResult,
    ReadSucceeded,
    TableRunReport,
    ValidationFailure,
    ValidationResult,
)

_AT = datetime(2026, 1, 1, tzinfo=UTC)
_NO_VALIDATION_FAILURES = ValidationResult()


def _table_report(
    *,
    read: ReadResult,
    validation: ValidationResult = _NO_VALIDATION_FAILURES,
    execution_results: tuple[ExecutionResult, ...] = (),
) -> TableRunReport:
    return TableRunReport(
        fully_qualified_name="cat.sch.tbl",
        started_at=_AT,
        ended_at=_AT,
        read=read,
        validation=validation,
        execution_results=execution_results,
    )


def test_renders_read_failure_detail():
    # Given: a table whose read phase failed
    report = _table_report(
        read=ReadFailed(ReadFailure("AnalysisException", "table not found"))
    )

    # When: rendering its failure detail
    lines = format_failure_detail(report)

    # Then: the headline and the read error line are present
    assert lines[0] == "\n❌ cat.sch.tbl [READ_FAILED]"
    assert "    Read error: AnalysisException - table not found" in lines


def test_renders_validation_failure_detail():
    # Given: a table whose validation phase failed
    report = _table_report(
        read=ReadSucceeded(observed=None),
        validation=ValidationResult(
            failures=(ValidationFailure("DisallowPartitioningChange", "cannot repartition"),)
        ),
    )

    # When: rendering its failure detail
    lines = format_failure_detail(report)

    # Then: the validation failure line is present
    assert lines[0] == "\n❌ cat.sch.tbl [VALIDATION_FAILED]"
    assert "    Validation failed: DisallowPartitioningChange - cannot repartition" in lines


def test_renders_execution_failure_detail_with_sql_preview():
    # Given: a table whose execution phase failed on one action
    failed_result = ExecutionResult(
        action="AddColumn",
        action_index=2,
        status=ActionStatus.FAILED,
        statement_preview="ALTER TABLE cat.sch.tbl ADD COLUMN x INT",
        failure=ExecutionFailure(action_index=2, exception_type="SparkException", message="boom"),
    )
    report = _table_report(read=ReadSucceeded(observed=None), execution_results=(failed_result,))

    # When: rendering its failure detail
    lines = format_failure_detail(report)

    # Then: both the failure line and the SQL preview are present
    assert lines[0] == "\n❌ cat.sch.tbl [EXECUTION_FAILED]"
    assert "    Execution failed at action 2: SparkException - boom" in lines
    assert "    Failed SQL preview (action 2):" in lines
    assert "        ALTER TABLE cat.sch.tbl ADD COLUMN x INT" in lines
