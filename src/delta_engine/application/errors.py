"""Application-level exception types for sync operations."""

from delta_engine.application.format_report import format_failure_detail
from delta_engine.application.results import SyncReport


class SyncFailedError(Exception):
    """Raised when one or more tables failed during sync."""

    def __init__(self, report: SyncReport) -> None:
        """Build a rich error message from the supplied sync `report`."""
        self.report = report

        failed_tables = [t for t in report.table_reports if t.has_failures]
        header = f"Sync failed: {len(failed_tables)}/{len(report.table_reports)} tables failed"

        details: list[str] = []
        for table_report in failed_tables:
            details.extend(format_failure_detail(table_report))

        super().__init__("\n".join([header, *details]))
