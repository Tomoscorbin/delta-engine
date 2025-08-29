"""Application-level error types."""

from __future__ import annotations

from delta_engine.application.validation import RunReport


class SyncFailedError(Exception):
    """Raised when one or more tables failed during sync."""

    def __init__(self, report: RunReport) -> None:
        self.report = report
        super().__init__(  # TODO: format properly
            f"Sync failed: {len(report.tables)} tables, "
            f"{sum(t.has_failures() for t in report.tables)} failed"
        )
