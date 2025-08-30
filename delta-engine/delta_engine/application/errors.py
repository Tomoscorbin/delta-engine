"""Application-level exception types for sync operations."""

from delta_engine.application.results import (
    ExecutionFailure,
    ReadFailure,
    SyncReport,
    ValidationFailure,
)


class SyncFailedError(Exception):
    """Raised when one or more tables failed during sync."""

    def __init__(self, report: SyncReport) -> None:
        self.report = report

        failed_tables = [t for t in report.table_reports if t.has_failures]
        num_failed = len(failed_tables)
        total = len(report.table_reports)

        header = f"Sync failed: {num_failed}/{total} tables failed"
        details: list[str] = []

        for t in failed_tables:
            # Table headline
            details.append(f"\n❌ {t.fully_qualified_name} [{t.status.value}]")

            # Top-level & aggregated failures (read, validation, execution)
            for fail in t.all_failures:
                if isinstance(fail, ReadFailure):
                    details.append(f"    Read error: {fail.exception_type} - {fail.message}")
                elif isinstance(fail, ValidationFailure):
                    details.append(f"    Validation failed: {fail.rule_name} - {fail.message}")
                elif isinstance(fail, ExecutionFailure):
                    details.append(
                        f"    Execution failed at action {fail.action_index}: "
                        f"{fail.exception_type} - {fail.message}"
                    )

            # Failed SQL previews for failed actions (if provided)
            if t.execution_results:
                for result in t.execution_results:
                    if result.failure:
                        details.append(f"    Failed SQL preview (action {result.action_index}):")
                        details.append(f"        {result.statement_preview}")

        super().__init__("\n".join([header] + details))
