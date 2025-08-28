from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ActionStatus(StrEnum):
    """Outcome status for an executed action."""
    OK = "OK"
    FAILED = "FAILED"
    NOOP = "NOOP"


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
class RunReport:
    """Aggregate report for a run across all tables."""
    run_id: str
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
            table_failures = tuple(
                e.failure for e in entries if e.failure is not None
            )
            if table_failures:
                failures[table] = table_failures
        return failures
