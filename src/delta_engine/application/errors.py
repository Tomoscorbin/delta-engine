"""
Application-level exception types for sync operations.

`SyncFailedError` owns how a failed run is communicated: it turns a
`SyncReport` into a human-readable summary, including per-table detail lines and
SQL previews for any failed actions.
"""

from __future__ import annotations

from delta_engine.application.results import ExecutionFailed, SyncReport, TableRunReport


class SyncFailedError(Exception):
    """Raised when one or more tables failed during sync."""

    def __init__(self, report: SyncReport) -> None:
        """Build a rich error message from the supplied sync `report`."""
        self.report = report

        failed_tables = [t for t in report.table_reports if t.has_failures]
        header = f"Sync failed: {len(failed_tables)}/{len(report.table_reports)} tables failed"

        details: list[str] = []
        for table_report in failed_tables:
            details.extend(_format_failure_detail(table_report))

        super().__init__("\n".join([header, *details]))


def _format_failure_detail(table_report: TableRunReport) -> list[str]:
    """
    Return the detail lines describing why a single table failed.

    Covers the table headline, each top-level failure (read, validation,
    execution), and SQL previews for any failed actions.
    """
    lines = [f"\n❌ {table_report.fully_qualified_name} [{table_report.status.value}]"]

    for failure in table_report.all_failures:
        lines.append(f"    {failure.format_line()}")

    # The surviving isinstance is structural: statement_preview lives on the
    # ExecutionFailed result arm, not on ExecutionFailure, so it cannot be
    # reached through execution.failures.
    for result in table_report.execution.results:
        if isinstance(result, ExecutionFailed):
            lines.append(f"    Failed SQL preview (action {result.action_index}):")
            lines.append(f"        {result.statement_preview}")

    return lines
