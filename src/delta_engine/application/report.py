"""
Run reports: per-table and run-level outcome aggregates.

`TableRunReport` carries one table's phase-ordered failure stream and derives
its status from the earliest failing phase; `SyncReport` aggregates a run.
"""

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from delta_engine.application.failures import (
    Failure,
    FailurePhase,
)
from delta_engine.application.ports import CatalogState, ExecutionSummary
from delta_engine.domain.model import DesiredTable, QualifiedName
from delta_engine.domain.plan import ActionPlan

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


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Aggregate report for a run across all tables."""

    started_at: datetime
    ended_at: datetime
    table_reports: tuple[TableRunReport, ...]
    dry_run: bool = False

    @property
    def any_failures(self) -> bool:
        """Return True if any table failed in the run."""
        return any(table_report.has_failures for table_report in self.table_reports)

    @property
    def failures_by_table(self) -> dict[QualifiedName, tuple[Failure, ...]]:
        """Mapping of qualified table name to its failures (if any)."""
        return {
            table_report.qualified_name: table_report.failures
            for table_report in self.table_reports
            if table_report.has_failures
        }

    def __iter__(self) -> Iterator[TableRunReport]:
        return iter(self.table_reports)
