"""
Application-owned exception types.

``ReadError`` and ``ExecutionError`` are transient signals raised by outbound
adapters and caught by the engine. The engine turns them into persistent
``Failure`` values for reports. ``DuplicateTableDefinitionError`` and
``SyncFailedError`` are raised from the inbound sync boundary to callers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from delta_engine.domain.model import QualifiedName

if TYPE_CHECKING:
    from delta_engine.application.report import SyncReport, TableRun


class ReadError(Exception):
    """A normalized backend error raised while reading one table's state."""

    def __init__(self, exception_type: str, message: str) -> None:
        """Initialize the error with backend-neutral diagnostic details."""
        super().__init__(message)
        self.exception_type = exception_type


class ExecutionError(Exception):
    """A normalized backend error raised while executing one statement."""

    def __init__(self, exception_type: str, message: str) -> None:
        """Initialize the error with backend-neutral diagnostic details."""
        super().__init__(message)
        self.exception_type = exception_type


class DuplicateTableDefinitionError(ValueError):
    """Raised when one sync receives two definitions for the same table."""

    def __init__(self, qualified_name: QualifiedName) -> None:
        """Name the duplicated table while preserving the ``ValueError`` contract."""
        self.qualified_name = qualified_name
        super().__init__(f"Duplicate table definition: {qualified_name}")


class SyncFailedError(Exception):
    """Raised when one or more tables failed during sync."""

    def __init__(self, report: SyncReport) -> None:
        """Build a rich error message from the supplied sync `report`."""
        self.report = report

        failed_runs = [run for run in report.table_runs if run.has_failures]
        header = f"Sync failed: {len(failed_runs)}/{len(report.table_runs)} tables failed"

        details: list[str] = []
        for run in failed_runs:
            details.extend(_format_failure_detail(run))

        super().__init__("\n".join([header, *details]))


def _format_failure_detail(run: TableRun) -> list[str]:
    """Return the detail lines describing why a single table failed."""
    lines = [f"\n{run.qualified_name} [{run.status.value}]"]
    for failure in run.failures:
        for line in failure.format_lines():
            lines.append(f"    {line}")
    return lines
