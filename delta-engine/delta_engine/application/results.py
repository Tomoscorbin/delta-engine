"""
Result and reporting types for engine runs.

Defines status enums, lightweight failure value objects, and aggregates used to
propagate outcomes from the read, validation, and execution phases. Provides
table- and run-level reports that summarize status, failures, and timing.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Union

from delta_engine.domain.model import ObservedTable

# ---------- Status enums ----------


class ActionStatus(StrEnum):
    """Outcome status for an executed action."""

    OK = "OK"
    FAILED = "FAILED"
    NOOP = "NOOP"


class TableRunStatus(StrEnum):
    """High-level status of a table's sync run."""
    SUCCESS = "SUCCESS"
    READ_FAILED = "READ_FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"


# ---------- Failure value objects ----------


@dataclass(frozen=True, slots=True)
class ReadFailure:
    """Failure reading current catalog state for a table."""

    exception_type: str
    message: str


@dataclass(frozen=True, slots=True)
class ValidationFailure:
    """Description of a validation rule failure."""

    rule_name: str
    message: str


@dataclass(frozen=True, slots=True)
class ExecutionFailure:
    """Details about a failed action execution."""

    action_index: int
    exception_type: str
    message: str


Failure = Union[ReadFailure, ValidationFailure, ExecutionFailure]


# ---------- ReadResult ----------


@dataclass(frozen=True, slots=True)
class ReadResult:
    """Outcome of reading current state for a table.

    Encodes one of three states: present with an observed schema; absent when
    the table does not exist; or failed with exception details.
    """
    observed: ObservedTable | None = None
    failure: ReadFailure | None = None

    @classmethod
    def create_present(cls, observed: ObservedTable) -> ReadResult:
        """Construct a successful read result with an observed table."""
        return cls(observed=observed)

    @classmethod
    def create_absent(cls) -> ReadResult:
        """Construct a successful read result indicating the object is missing."""
        return cls()

    @classmethod
    def create_failed(cls, failure: ReadFailure) -> ReadResult:
        """Construct a failed read result with failure details."""
        return cls(failure=failure)

    @property
    def failed(self) -> bool:
        """True when the read encountered a failure."""
        return self.failure is not None


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
class ExecutionResult:  # do we want an ActionResult and then an aggregate ExecutionResult?
    """Result of executing a single action in a plan."""

    action: str
    action_index: int
    status: ActionStatus
    statement_preview: str
    failure: ExecutionFailure | None = None

    def __post_init__(self) -> None:
        """Enforce consistency between status and presence of `failure`."""
        if (
            self.status is ActionStatus.FAILED and self.failure is None
        ):  # are these really necessary?
            raise ValueError("FAILED ExecutionResult must include an ExecutionFailure.")
        if self.status is not ActionStatus.FAILED and self.failure is not None:
            raise ValueError("Only FAILED results may include an ExecutionFailure.")


# ---------- Reports ----------


@dataclass(frozen=True, slots=True)
class TableRunReport:
    """Per-table report with timings, outcomes, and failures."""
    fully_qualified_name: str
    started_at: datetime
    ended_at: datetime
    read: ReadResult
    validation: ValidationResult
    execution_results: tuple[ExecutionResult, ...] = ()

    @property
    def _any_action_failed(self) -> bool:
        """True if any execution result has status=FAILED."""
        return any(e.status is ActionStatus.FAILED for e in self.execution_results)

    @property
    def status(self) -> TableRunStatus:
        """Aggregate table status across read, validation, and execution phases."""
        if self.read.failed:
            return TableRunStatus.READ_FAILED
        if self.validation.failed:
            return TableRunStatus.VALIDATION_FAILED
        if self._any_action_failed:
            return TableRunStatus.EXECUTION_FAILED
        return TableRunStatus.SUCCESS

    @property
    def has_failures(self) -> bool:
        """True if the table did not fully succeed."""
        return self.status is not TableRunStatus.SUCCESS

    @property
    def action_failures(self) -> tuple[ExecutionFailure, ...]:
        """Failures captured from action execution results only."""
        return tuple(e.failure for e in self.execution_results if e.status is ActionStatus.FAILED)

    @property
    def all_failures(self) -> tuple[Failure, ...]:
        """All failures for this table (read, validation, execution)."""
        out: list[Failure] = []
        if self.read.failed:
            out.append(self.read.failure)
        if self.validation.failed:
            out.extend(self.validation.failures)
        out.extend(self.action_failures)
        return tuple(out)


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
    def failures_by_table(self) -> dict[str, tuple[Failure, ...]]:
        """Mapping of fully qualified table name to its failures (if any)."""
        return {
            t.fully_qualified_name: t.all_failures for t in self.table_reports if t.has_failures
        }

    def __iter__(self) -> Iterator[TableRunReport]:
        """Iterate over per-table reports in the sync run."""
        return iter(self.table_reports)
