"""
Result and reporting types for engine runs.

Defines status enums, lightweight failure value objects, and aggregates used to
propagate outcomes from the foreign-key resolution, read, validation, and
execution phases. Provides table- and run-level reports that summarize status,
failures, and timing.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from delta_engine.application.failures import (
    Failure,
    FailurePhase,
)
from delta_engine.application.ports import CatalogState, ExecutionSummary
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import ActionPlan

# ---------- Status enums ----------


class TableRunStatus(StrEnum):
    """High-level status of a table's sync run."""

    SUCCESS = "SUCCESS"
    READ_FAILED = "READ_FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    FOREIGN_KEY_FAILED = "FOREIGN_KEY_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"


# ---------- Reports ----------


_STATUS_FOR_PHASE: dict[FailurePhase, TableRunStatus] = {
    FailurePhase.READ: TableRunStatus.READ_FAILED,
    FailurePhase.VALIDATION: TableRunStatus.VALIDATION_FAILED,
    FailurePhase.FOREIGN_KEY: TableRunStatus.FOREIGN_KEY_FAILED,
    FailurePhase.EXECUTION: TableRunStatus.EXECUTION_FAILED,
}


@dataclass(frozen=True, slots=True)
class TableRunReport:
    """Per-table report with outcomes and a single phase-ordered failure stream."""

    qualified_name: QualifiedName
    desired: DesiredTable
    read: CatalogState
    plan: ActionPlan = field(default_factory=ActionPlan)
    failures: tuple[Failure, ...] = ()
    execution: ExecutionSummary | None = None

    @property
    def status(self) -> TableRunStatus:
        """Status of the earliest phase that failed; SUCCESS when nothing failed."""
        if not self.failures:
            return TableRunStatus.SUCCESS
        return _STATUS_FOR_PHASE[min(failure.phase for failure in self.failures)]

    @property
    def has_failures(self) -> bool:
        """True if the table did not fully succeed."""
        return bool(self.failures)

    def diff(self) -> str:
        """Render this table's planned changes as a +/-/~ change list."""
        from delta_engine.application.rendering import render_diff_block

        return render_diff_block(self)

    def __str__(self) -> str:
        """Render this report as the grid header plus its single row."""
        from delta_engine.application.rendering import render_grid

        return render_grid((self,))


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Aggregate report for a run across all tables."""

    started_at: datetime
    ended_at: datetime
    table_reports: tuple[TableRunReport, ...]

    @property
    def any_failures(self) -> bool:
        """Return True if any table failed in the run."""
        return any(t.has_failures for t in self.table_reports)

    @property
    def failures_by_table(self) -> dict[QualifiedName, tuple[Failure, ...]]:
        """Mapping of qualified table name to its failures (if any)."""
        return {t.qualified_name: t.failures for t in self.table_reports if t.has_failures}

    def __iter__(self) -> Iterator[TableRunReport]:
        return iter(self.table_reports)

    def diff(self) -> str:
        """Render every table's planned changes, in report order."""
        from delta_engine.application.rendering import render_diff_block

        return "\n\n".join(render_diff_block(report) for report in self.table_reports)

    def __str__(self) -> str:
        """Render the run as an aligned grid followed by a summary footer."""
        from delta_engine.application.rendering import render_grid, run_summary_footer

        if not self.table_reports:
            return "Sync report: 0 tables"
        return f"{render_grid(self.table_reports)}\n\n{run_summary_footer(self)}"
