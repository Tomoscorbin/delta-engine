"""
Run reports: per-table and run-level outcome aggregates.

`TableRunReport` is the immutable public snapshot created from one completed
engine run; `SyncReport` aggregates those table snapshots.
"""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Self

from delta_engine.application.diff_entries import action_entries
from delta_engine.application.failures import (
    Failure,
    FailurePhase,
    ReadFailure,
)
from delta_engine.application.ports import ExecutionSummary, ReadResult
from delta_engine.domain.model import DesiredTable, QualifiedName
from delta_engine.domain.plan import ActionPlan

# ---------- Status enums ----------


class TableRunStatus(StrEnum):
    """High-level status of a table's sync run."""

    SUCCESS = "SUCCESS"
    READ_FAILED = "READ_FAILED"
    PLANNING_FAILED = "PLANNING_FAILED"
    FOREIGN_KEY_FAILED = "FOREIGN_KEY_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"


# ---------- Reports ----------


_STATUS_FOR_PHASE: Final[Mapping[FailurePhase, TableRunStatus]] = MappingProxyType(
    {
        FailurePhase.READ: TableRunStatus.READ_FAILED,
        FailurePhase.PLANNING: TableRunStatus.PLANNING_FAILED,
        FailurePhase.FOREIGN_KEY: TableRunStatus.FOREIGN_KEY_FAILED,
        FailurePhase.EXECUTION: TableRunStatus.EXECUTION_FAILED,
    }
)


def _change_records(plan: ActionPlan) -> list[dict[str, str]]:
    """
    Summarise the plan as flat change records, in plan order.

    These share the interpretation vocabulary of the text renderers: they are
    human-oriented summaries, not one record per action (a CreateTable expands
    into several), and not a complete description of the change — the
    authoritative description is the planned SQL.
    """
    return [
        {
            "kind": entry.category.name.lower(),
            "operation": entry.operation,
            "subject": entry.subject,
            "detail": entry.detail,
        }
        for action in plan
        for entry in action_entries(action)
    ]


def _failure_records(failures: tuple[Failure, ...]) -> list[dict[str, str]]:
    """Project failures as flat records, in phase order as carried."""
    return [
        {
            "phase": failure.phase.name,
            "type": type(failure).__name__,
            "message": " ".join(line.strip() for line in failure.format_lines()),
        }
        for failure in failures
    ]


@dataclass(frozen=True, slots=True, init=False)
class TableRunReport:
    """
    Frozen public projection of one completed table run.

    The engine creates reports through ``_create`` after all phases finish.
    Keeping construction controlled means the plan, failures, and execution
    summary are frozen together and cannot be supplied as contradictory public
    constructor arguments. ``planned_sql_statements`` is populated on dry and
    real runs so planned changes remain inspectable even when execution is
    skipped or blocked.
    """

    desired: DesiredTable
    read: ReadResult
    plan: ActionPlan
    planned_sql_statements: tuple[str, ...]
    failures: tuple[Failure, ...]
    execution: ExecutionSummary | None

    def __init__(self) -> None:
        raise TypeError("TableRunReport values are created by Engine.sync")

    @classmethod
    def _create(
        cls,
        *,
        desired: DesiredTable,
        read: ReadResult,
        plan: ActionPlan,
        planned_sql_statements: tuple[str, ...],
        failures: tuple[Failure, ...],
        execution: ExecutionSummary | None,
    ) -> Self:
        """Freeze one internally consistent table-run projection."""
        statements = tuple(planned_sql_statements)
        frozen_failures = tuple(failures)

        expected_read_failures = (read,) if isinstance(read, ReadFailure) else ()
        read_failures = tuple(
            failure for failure in frozen_failures if failure.phase is FailurePhase.READ
        )
        if read_failures != expected_read_failures:
            raise ValueError("Read failures must match the retained read result")

        expected_execution_failures = () if execution is None else execution.failures
        execution_failures = tuple(
            failure for failure in frozen_failures if failure.phase is FailurePhase.EXECUTION
        )
        if execution_failures != expected_execution_failures:
            raise ValueError("Execution failures must match the execution summary")

        if execution is not None:
            if any(failure.phase is not FailurePhase.EXECUTION for failure in frozen_failures):
                raise ValueError("Execution cannot follow an earlier phase failure")
            if len(execution.results) > len(statements):
                raise ValueError("Execution cannot contain more results than planned statements")
            if (
                tuple(result.statement for result in execution.results)
                != statements[: len(execution.results)]
            ):
                raise ValueError("Execution results must match the planned statement prefix")

        report = object.__new__(cls)
        object.__setattr__(report, "desired", desired)
        object.__setattr__(report, "read", read)
        object.__setattr__(report, "plan", plan)
        object.__setattr__(report, "planned_sql_statements", statements)
        object.__setattr__(report, "failures", frozen_failures)
        object.__setattr__(report, "execution", execution)
        return report

    @property
    def qualified_name(self) -> QualifiedName:
        """The table identity from the declaration retained by this report."""
        return self.desired.qualified_name

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

    @property
    def has_changes(self) -> bool:
        """True when the plan holds actions — drift was found and validated."""
        return bool(self.plan)

    def to_dict(self) -> dict[str, Any]:
        """
        Project this table's run as plain, JSON-serialisable data.

        The field names are a public stability contract (see the run report
        reference doc); changing them is a breaking change.
        """
        if self.execution is None:
            execution_record: dict[str, int] | None = None
        else:
            execution_record = {
                "applied": self.execution.applied_count,
                "total": len(self.planned_sql_statements),
            }
        return {
            "name": str(self.qualified_name),
            "status": self.status.value,
            "has_changes": self.has_changes,
            "has_failures": self.has_failures,
            "changes": _change_records(self.plan),
            "planned_sql_statements": list(self.planned_sql_statements),
            "failures": _failure_records(self.failures),
            "execution": execution_record,
        }


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Aggregate report for a run across all tables."""

    started_at: datetime
    ended_at: datetime
    table_reports: tuple[TableRunReport, ...]
    dry_run: bool = False

    @property
    def has_failures(self) -> bool:
        """Return True if any table failed in the run."""
        return any(table_report.has_failures for table_report in self.table_reports)

    @property
    def has_changes(self) -> bool:
        """
        True if any table's plan holds actions.

        States facts about *planned* changes only: a table that failed
        validation contributes failures, not changes. The CI gate idiom is
        ``report.has_failures or report.has_changes``.
        """
        return any(table_report.has_changes for table_report in self.table_reports)

    @property
    def planned_sql_statements(self) -> dict[str, tuple[str, ...]]:
        """Dotted table name → the SQL its plan compiles to; no-op tables omitted."""
        return {
            str(table_report.qualified_name): table_report.planned_sql_statements
            for table_report in self.table_reports
            if table_report.planned_sql_statements
        }

    @property
    def failures_by_table(self) -> dict[QualifiedName, tuple[Failure, ...]]:
        """Mapping of qualified table name to its failures (if any)."""
        return {
            table_report.qualified_name: table_report.failures
            for table_report in self.table_reports
            if table_report.has_failures
        }

    def to_dict(self) -> dict[str, Any]:
        """Project the whole run as plain, JSON-serialisable data; tables in run order."""
        return {
            "schema_version": 2,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "dry_run": self.dry_run,
            "has_changes": self.has_changes,
            "has_failures": self.has_failures,
            "tables": [table_report.to_dict() for table_report in self.table_reports],
        }

    def __iter__(self) -> Iterator[TableRunReport]:
        return iter(self.table_reports)
