"""
Result and reporting types for engine runs.

Defines status enums, lightweight failure value objects, and aggregates used to
propagate outcomes from the read, validation, and execution phases. Provides
table- and run-level reports that summarize status, failures, and timing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from delta_engine.domain.model import ObservedTable, QualifiedName

# ---------- Status enums ----------


class TableRunStatus(StrEnum):
    """High-level status of a table's sync run."""

    SUCCESS = "SUCCESS"
    READ_FAILED = "READ_FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"


class SkipReason(StrEnum):
    """Why a foreign key constraint was skipped during sync."""

    CYCLE = "CYCLE"
    UNRESOLVABLE_REFERENCE = "UNRESOLVABLE_REFERENCE"


@dataclass(frozen=True, slots=True)
class SkippedForeignKey:
    """A foreign key constraint that was not applied due to a graph-level reason."""

    table: QualifiedName
    constraint_name: str
    reason: SkipReason


@dataclass(frozen=True, slots=True)
class ForeignKeyValidationReport:
    """Cross-table FK graph analysis results."""

    skipped: tuple[SkippedForeignKey, ...] = ()

    @property
    def has_skipped(self) -> bool:
        """True when any FK constraints were skipped."""
        return bool(self.skipped)


# ---------- Failure value objects ----------


class Failure(ABC):
    """A failure that can render itself as display lines."""

    @abstractmethod
    def format_lines(self) -> tuple[str, ...]:
        """Return one or more human-readable lines describing this failure."""
        ...


@dataclass(frozen=True, slots=True)
class ReadFailure(Failure):
    """Failure reading current catalog state for a table."""

    exception_type: str
    message: str

    def format_lines(self) -> tuple[str, ...]:
        return (f"Read error: {self.exception_type} - {self.message}",)


@dataclass(frozen=True, slots=True)
class ValidationFailure(Failure):
    """Description of a validation rule failure."""

    rule_name: str
    message: str

    def format_lines(self) -> tuple[str, ...]:
        return (f"Validation failed: {self.rule_name} - {self.message}",)


@dataclass(frozen=True, slots=True)
class ExecutionFailure(Failure):
    """Details about a failed action execution."""

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
    action_index: int
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


@dataclass(frozen=True, slots=True)
class TableRunReport:
    """Per-table report with timings, outcomes, and failures."""

    qualified_name: QualifiedName
    started_at: datetime
    ended_at: datetime
    read: CatalogState
    validation: ValidationResult
    execution: ExecutionSummary = field(default_factory=ExecutionSummary)

    @property
    def status(self) -> TableRunStatus:
        """Aggregate table status across read, validation, and execution phases."""
        if isinstance(self.read, ReadFailed):
            return TableRunStatus.READ_FAILED
        if self.validation.failed:
            return TableRunStatus.VALIDATION_FAILED
        if self.execution.failed:
            return TableRunStatus.EXECUTION_FAILED
        return TableRunStatus.SUCCESS

    @property
    def has_failures(self) -> bool:
        """True if the table did not fully succeed."""
        return self.status is not TableRunStatus.SUCCESS

    @property
    def all_failures(self) -> tuple[Failure, ...]:
        """All failures for this table (read, validation, execution)."""
        out: list[Failure] = []
        if isinstance(self.read, ReadFailed):
            out.append(self.read.failure)
        out.extend(self.validation.failures)
        out.extend(self.execution.failures)
        return tuple(out)


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Aggregate report for a run across all tables."""

    started_at: datetime
    ended_at: datetime
    table_reports: tuple[TableRunReport, ...]
    foreign_key_validation: ForeignKeyValidationReport = field(
        default_factory=ForeignKeyValidationReport
    )

    @property
    def any_failures(self) -> bool:
        """Return True if any table failed in the run."""
        return any(t.has_failures for t in self.table_reports)

    @property
    def failures_by_table(self) -> dict[QualifiedName, tuple[Failure, ...]]:
        """Mapping of qualified table name to its failures (if any)."""
        return {t.qualified_name: t.all_failures for t in self.table_reports if t.has_failures}

    def __iter__(self) -> Iterator[TableRunReport]:
        return iter(self.table_reports)
