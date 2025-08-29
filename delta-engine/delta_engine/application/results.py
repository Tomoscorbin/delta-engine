from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Union

from delta_engine.domain.model import ObservedTable


class ActionStatus(StrEnum):
    """Outcome status for an executed action."""

    OK = "OK"
    FAILED = "FAILED"
    NOOP = "NOOP"


class TableRunStatus(StrEnum):
    SUCCESS = "SUCCESS"
    READ_FAILED = "READ_FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"


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


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Result of executing a single action in a plan."""

    action: str
    action_index: int
    status: ActionStatus
    statement_preview: str
    failure: ExecutionFailure | None = None


@dataclass(frozen=True, slots=True)
class CatalogReadResult:
    observed: ObservedTable | None = None
    failure: ReadFailure | None = None

    @classmethod
    def present(cls, observed: ObservedTable) -> CatalogReadResult:
        return cls(observed=observed)

    @classmethod
    def absent(cls) -> CatalogReadResult:
        return cls()

    @classmethod
    def failed(cls, failure: ReadFailure) -> CatalogReadResult:
        return cls(failure=failure)


Failure = Union[ReadFailure, ValidationFailure, ExecutionFailure]


@dataclass(frozen=True, slots=True)
class TableRunReport:
    fully_qualified_name: str
    started_at: str
    ended_at: str
    execution_results: tuple[ExecutionResult, ...] | None = None
    failure: tuple[Failure] | None = None

    @property
    def status(self) -> TableRunStatus:
        failure = self.failure
        if isinstance(failure, ReadFailure):
            return TableRunStatus.READ_FAILED
        if isinstance(failure, ValidationFailure):
            return TableRunStatus.VALIDATION_FAILED
        if isinstance(failure, ExecutionFailure):
            return TableRunStatus.EXECUTION_FAILED
        else:
            return TableRunStatus.SUCCESS

    def has_failures(self) -> bool:
        return self.failure is not None


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Aggregate report for a run across all tables."""

    started_at: str
    ended_at: str
    executions_by_table: dict[str, tuple[ExecutionResult, ...]]

    @property
    def any_failures(self) -> bool:
        """Return ``True`` if any action failed in the run."""
        return any(
            entry.failure is not None
            for entries in self.executions_by_table.values()
            for entry in entries
        )

    @property
    def failures_by_table(self) -> dict[str, ExecutionFailure]:
        """Return a mapping of table name to its execution failures."""
        failures: dict[str, tuple[ExecutionFailure, ...]] = {}
        for table, entries in self.executions_by_table.items():
            table_failures = tuple(e.failure for e in entries if e.failure is not None)
            if table_failures:
                failures[table] = table_failures
        return failures
