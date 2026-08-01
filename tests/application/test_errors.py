from datetime import UTC, datetime

from delta_engine.application.errors import (
    DuplicateTableDefinitionError,
    ExecutionError,
    ReadError,
    SyncFailedError,
)
from delta_engine.application.failures import (
    ExecutionFailure,
    Failure,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.planning import PlanningFailed, PlanningSucceeded
from delta_engine.application.ports import (
    CompiledPlan,
    ExecutionSucceeded,
    ExecutionSummary,
    ReadResult,
    TableAbsent,
)
from delta_engine.application.relationships import TableResolution
from delta_engine.application.report import (
    SyncReport,
    TableRunReport,
)
from delta_engine.domain.model import DesiredColumn, Integer, QualifiedName
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan import ActionPlan, SetTableComment
from tests.builders import build_compiled_plan

_AT = datetime(2026, 1, 1, tzinfo=UTC)
_QN = QualifiedName("cat", "sch", "tbl")


def _compiled(*statements: str) -> CompiledPlan:
    plan = ActionPlan(
        target=_QN,
        actions=tuple(
            SetTableComment(desired_comment=f"new {index}", observed_comment=f"old {index}")
            for index in range(len(statements))
        ),
    )
    return build_compiled_plan(plan, statements)


def test_duplicate_table_definition_error_is_a_value_error_with_the_name():
    error = DuplicateTableDefinitionError(_QN)

    assert isinstance(error, ValueError)
    assert error.qualified_name == _QN
    assert str(error) == "Duplicate table definition: cat.sch.tbl"


def test_port_errors_are_regular_exceptions_with_normalized_details():
    for error in (
        ReadError(exception_type="AnalysisException", message="cannot read"),
        ExecutionError(exception_type="SparkException", message="cannot execute"),
    ):
        assert isinstance(error, Exception)
        assert error.exception_type.endswith("Exception")
        assert str(error).startswith("cannot")


def _table_report(
    *,
    read: ReadResult,
    failures: tuple[Failure, ...] = (),
    execution: ExecutionSummary | None = None,
) -> TableRunReport:
    desired = DesiredTable(qualified_name=_QN, columns=(DesiredColumn("id", Integer()),))
    planning_failures = tuple(
        failure for failure in failures if isinstance(failure, ValidationFailure)
    )
    resolution_failures = tuple(
        failure for failure in failures if isinstance(failure, ForeignKeyFailure)
    )
    if isinstance(read, ReadFailure):
        planning = None
        compiled = None
    elif planning_failures:
        planning = PlanningFailed(planning_failures)
        compiled = None
    else:
        compiled = execution.compiled_plan if execution is not None else _compiled()
        planning = PlanningSucceeded(compiled.plan)
    resolution = TableResolution(
        desired=desired,
        dependencies=(),
        structural_failures=resolution_failures,
    )

    return TableRunReport(
        read=read,
        planning=planning,
        compiled=compiled,
        resolution=resolution,
        execution=execution,
    )


def _message_for(table_report: TableRunReport) -> str:
    """Return the SyncFailedError message produced for a run with this one failed table."""
    report = SyncReport(started_at=_AT, ended_at=_AT, table_reports=(table_report,))
    return str(SyncFailedError(report))


def test_message_headline_counts_failed_tables():
    # Given a run with a single failed table
    report = _table_report(
        read=ReadFailure("AnalysisException", "table not found"),
    )

    # When building the error message
    message = _message_for(report)

    # Then the headline reports the failed/total tally
    assert message.startswith("Sync failed: 1/1 tables failed")


def test_message_renders_read_failure_detail():
    # Given a table whose read phase failed
    report = _table_report(
        read=ReadFailure("AnalysisException", "table not found"),
    )

    # When building the error message
    message = _message_for(report)

    # Then the table headline and the read error line are present
    assert "cat.sch.tbl [READ_FAILED]" in message
    assert "❌" not in message
    assert "Read error: AnalysisException - table not found" in message


def test_message_renders_validation_failure_detail():
    # Given a table whose validation phase failed
    report = _table_report(
        read=TableAbsent(),
        failures=(ValidationFailure("DisallowPartitioningChange", "cannot repartition"),),
    )

    # When building the error message
    message = _message_for(report)

    # Then the validation failure line is present
    assert "cat.sch.tbl [PLANNING_FAILED]" in message
    assert "Validation failed: DisallowPartitioningChange - cannot repartition" in message


def test_message_renders_every_validation_failure_when_a_table_breaks_several_rules():
    # Given a table whose plan tripped two rules at once
    report = _table_report(
        read=TableAbsent(),
        failures=(
            ValidationFailure("NonNullableColumnAdd", "cannot add NOT NULL column 'age'"),
            ValidationFailure("DisallowPartitioningChange", "cannot repartition"),
        ),
    )

    # When building the error message
    message = _message_for(report)

    # Then BOTH failures are surfaced together, not just the first
    assert "Validation failed: NonNullableColumnAdd - cannot add NOT NULL column 'age'" in message
    assert "Validation failed: DisallowPartitioningChange - cannot repartition" in message


def test_message_renders_execution_failure_detail_with_sql():
    # Given a table whose execution phase failed on one action
    statements = (
        "ALTER TABLE cat.sch.tbl STEP 0",
        "ALTER TABLE cat.sch.tbl STEP 1",
        "ALTER TABLE cat.sch.tbl ADD COLUMN x INT",
    )
    compiled = _compiled(*statements)
    failed_result = ExecutionFailure(
        statement_index=2,
        exception_type="SparkException",
        message="boom",
        statement=statements[2],
    )
    report = _table_report(
        read=TableAbsent(),
        execution=ExecutionSummary(
            compiled_plan=compiled,
            results=(
                ExecutionSucceeded(0, statements[0]),
                ExecutionSucceeded(1, statements[1]),
                failed_result,
            ),
        ),
    )

    # When building the error message
    message = _message_for(report)

    # Then both the failure line and the SQL statement is present
    assert "cat.sch.tbl [EXECUTION_FAILED]" in message
    assert "Execution failed at statement 3: SparkException - boom" in message
    assert "ALTER TABLE cat.sch.tbl ADD COLUMN x INT" in message


def test_message_renders_fk_failure_detail():
    # Given a table with an FK failure described by its content
    report = _table_report(
        read=TableAbsent(),
        failures=(
            ForeignKeyFailure(
                table=_QN,
                local_columns=("ref_id",),
                references=QualifiedName("cat", "sch", "other"),
                reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
            ),
        ),
    )

    # When building the error message
    message = _message_for(report)

    # Then the FK failure line is present with content-based description
    assert "cat.sch.tbl [FOREIGN_KEY_FAILED]" in message
    assert "(ref_id)" in message
    assert "not registered" in message
