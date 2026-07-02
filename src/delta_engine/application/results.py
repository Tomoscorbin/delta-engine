"""
Result and reporting types for engine runs.

Defines status enums, lightweight failure value objects, and aggregates used to
propagate outcomes from the foreign-key resolution, read, validation, and
execution phases. Provides table- and run-level reports that summarize status,
failures, and timing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum, StrEnum
from typing import ClassVar

from delta_engine.domain.model import ObservedTable, QualifiedName
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import ActionPlan


class FailurePhase(IntEnum):
    """The sync phase that produced a failure. Ordered so the earliest wins."""

    READ = 1
    VALIDATION = 2
    FOREIGN_KEY = 3
    EXECUTION = 4


# ---------- Status enums ----------


class TableRunStatus(StrEnum):
    """High-level status of a table's sync run."""

    SUCCESS = "SUCCESS"
    READ_FAILED = "READ_FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    FOREIGN_KEY_FAILED = "FOREIGN_KEY_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"


class ForeignKeyFailureReason(StrEnum):
    """Why a foreign key constraint could not be applied, failing its whole table."""

    CYCLE = "CYCLE"
    UNRESOLVABLE_REFERENCE = "UNRESOLVABLE_REFERENCE"
    BLOCKED_BY_FAILED_DEPENDENCY = "BLOCKED_BY_FAILED_DEPENDENCY"
    REFERENCED_COLUMNS_NOT_A_KEY = "REFERENCED_COLUMNS_NOT_A_KEY"

    @property
    def detail(self) -> str:
        """Human-readable reason clause for a failure message."""
        match self:
            case ForeignKeyFailureReason.CYCLE:
                return "it is part of a foreign key dependency cycle"
            case ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE:
                return "it references a table that is not registered"
            case ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY:
                return "it references a table that failed to sync"
            case ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY:
                return "its referenced columns are not the primary key of the referenced table"


# ---------- Failure value objects ----------


class Failure(ABC):
    """A failure that can render itself as display lines, tagged with its phase."""

    phase: ClassVar[FailurePhase]

    @abstractmethod
    def format_lines(self) -> tuple[str, ...]:
        """Return one or more human-readable lines describing this failure."""
        ...


@dataclass(frozen=True, slots=True)
class ReadFailure(Failure):
    """Failure reading current catalog state for a table."""

    phase: ClassVar[FailurePhase] = FailurePhase.READ
    exception_type: str
    message: str

    def format_lines(self) -> tuple[str, ...]:
        return (f"Read error: {self.exception_type} - {self.message}",)


@dataclass(frozen=True, slots=True)
class ValidationFailure(Failure):
    """Description of a validation rule failure."""

    phase: ClassVar[FailurePhase] = FailurePhase.VALIDATION
    rule_name: str
    message: str

    def format_lines(self) -> tuple[str, ...]:
        return (f"Validation failed: {self.rule_name} - {self.message}",)


@dataclass(frozen=True, slots=True)
class ExecutionFailure(Failure):
    """Details about a failed action execution."""

    phase: ClassVar[FailurePhase] = FailurePhase.EXECUTION
    action_index: int
    exception_type: str
    message: str
    statement_preview: str

    def format_lines(self) -> tuple[str, ...]:
        return (
            f"Execution failed at action {self.action_index}: "
            f"{self.exception_type} - {self.message}",
            f"    SQL preview: {self.statement_preview}",
        )


@dataclass(frozen=True, slots=True)
class ForeignKeyFailure(Failure):
    """A foreign key constraint that could not be applied, failing its whole table."""

    phase: ClassVar[FailurePhase] = FailurePhase.FOREIGN_KEY
    table: QualifiedName
    local_columns: tuple[str, ...]
    references: str
    reason: ForeignKeyFailureReason

    def format_lines(self) -> tuple[str, ...]:
        columns = ", ".join(self.local_columns)
        return (
            f"Foreign key ({columns}) → {self.references} on {self.table} was not applied: "
            f"{self.reason.detail}.",
        )


# ---------- CatalogState ----------


@dataclass(frozen=True, slots=True)
class TablePresent:
    """The catalog holds a live table; ``table`` is its observed schema."""

    table: ObservedTable


@dataclass(frozen=True, slots=True)
class TableAbsent:
    """The catalog confirmed the table does not exist; the engine will create it."""


@dataclass(frozen=True, slots=True)
class ReadFailed:
    """A catalog read that raised before any state could be determined."""

    failure: ReadFailure


# The three answers a catalog can give about a table: it is there, it is not
# there, or it could not be read.
CatalogState = TablePresent | TableAbsent | ReadFailed


# ---------- ValidationResult ----------


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Outcome of plan validation."""

    failures: tuple[ValidationFailure, ...] = ()

    @property
    def failed(self) -> bool:
        """True when any validation failures are present."""
        return bool(self.failures)


# ---------- ExecutionResult ----------


@dataclass(frozen=True, slots=True)
class ExecutionSucceeded:
    """A single plan action that executed without error."""

    action: str
    action_index: int
    statement_preview: str


@dataclass(frozen=True, slots=True)
class ExecutionFailed:
    """A single plan action that raised while executing."""

    action: str
    failure: ExecutionFailure


# An executed action either succeeds or fails. The split makes "succeeded but
# carries a failure" (and "failed but carries none") unrepresentable, so no
# runtime invariant guard is needed.
ExecutionResult = ExecutionSucceeded | ExecutionFailed


@dataclass(frozen=True, slots=True)
class ExecutionSummary:
    """
    The outcome of running a whole action plan.

    Mirrors :class:`ValidationResult`: a frozen container over the phase's raw
    results that answers ``failed`` and exposes its ``failures``. It owns the
    single pass that separates failed actions from successful ones, so callers
    read a property instead of re-deriving the split with ``isinstance``.
    """

    results: tuple[ExecutionResult, ...] = ()

    @property
    def failed(self) -> bool:
        """True when any action in the plan failed."""
        return any(isinstance(result, ExecutionFailed) for result in self.results)

    @property
    def failures(self) -> tuple[ExecutionFailure, ...]:
        """The failure detail from each failed action, in execution order."""
        return tuple(
            result.failure for result in self.results if isinstance(result, ExecutionFailed)
        )

    @property
    def failed_count(self) -> int:
        """How many of the plan's actions failed."""
        return len(self.failures)


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
