"""
Run reports: per-table and run-level outcome aggregates.

`TableRunReport` is the immutable public snapshot created from one completed
engine run; `SyncReport` aggregates those table snapshots.
"""

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, NamedTuple

from delta_engine.application.diff_entries import DiffEntry, drift_entries, plan_entries
from delta_engine.application.failures import (
    Failure,
    FailurePhase,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
)
from delta_engine.application.planning import (
    PlanningFailed,
    PlanningResult,
    accepted_plan,
)
from delta_engine.application.ports import CompiledPlan, ExecutionSummary, ReadResult
from delta_engine.application.relationships import TableResolution
from delta_engine.domain.model import DesiredTable, QualifiedName
from delta_engine.domain.plan import ActionPlan, CreateTable, TableDiff, TableDrift

# ---------- Status enums ----------


class TableRunStatus(StrEnum):
    """High-level status of a table's sync run."""

    SUCCESS = "SUCCESS"
    READ_FAILED = "READ_FAILED"
    PLANNING_FAILED = "PLANNING_FAILED"
    FOREIGN_KEY_FAILED = "FOREIGN_KEY_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"


# ---------- Derived run facts ----------


class StatementProgress(NamedTuple):
    """How many of a table's planned statements execution actually applied."""

    applied: int
    planned: int


class RunCounts(NamedTuple):
    """
    How a run's tables came out.

    Every table falls in exactly one bucket, so ``total`` is their sum rather
    than a separately counted fact that could disagree with them.
    """

    changed: int
    unchanged: int
    failed: int

    @property
    def total(self) -> int:
        """Tables in the run."""
        return self.changed + self.unchanged + self.failed


# ---------- Reports ----------


_STATUS_FOR_PHASE: Final[Mapping[FailurePhase, TableRunStatus]] = MappingProxyType(
    {
        FailurePhase.READ: TableRunStatus.READ_FAILED,
        FailurePhase.PLANNING: TableRunStatus.PLANNING_FAILED,
        FailurePhase.FOREIGN_KEY: TableRunStatus.FOREIGN_KEY_FAILED,
        FailurePhase.EXECUTION: TableRunStatus.EXECUTION_FAILED,
    }
)


def _entry_records(entries: Iterable[DiffEntry]) -> list[dict[str, str]]:
    """Project interpreted diff entries as flat records, in the order given."""
    return [
        {
            "kind": entry.category.name.lower(),
            "operation": entry.operation.value,
            "subject": entry.subject,
            "detail": " ".join(entry.detail),
        }
        for entry in entries
    ]


def _change_records(plan: ActionPlan | None) -> list[dict[str, str]]:
    """
    Summarise the plan as flat change records, in plan order.

    These share the interpretation vocabulary of the text renderers: they are
    human-oriented summaries, not one record per action (a CreateTable expands
    into several), and not a complete description of the change — the
    authoritative description is the planned SQL.
    """
    if plan is None:
        return []
    return _entry_records(plan_entries(plan))


def _rejected_change_records(
    diff: TableDiff | None, plan: ActionPlan | None
) -> list[dict[str, str]]:
    """
    Summarise the differences a rejected diff found, in diff order.

    Empty whenever a plan exists: an accepted diff's differences are its
    changes, already projected by ``_change_records``.
    """
    if plan is not None or not isinstance(diff, TableDrift):
        return []
    return _entry_records(drift_entries(diff))


def _failure_records(failures: tuple[Failure, ...]) -> list[dict[str, str]]:
    """Project failures as flat records, in phase order as carried."""
    return [
        {
            "phase": failure.phase.name,
            "type": type(failure).__name__,
            "message": " ".join(failure.format_lines()),
        }
        for failure in failures
    ]


@dataclass(frozen=True, slots=True)
class TableRunReport:
    """
    Frozen public projection of one completed table run.

    The engine creates a report after all phases finish, projecting its
    canonical phase outcomes into this immutable snapshot.
    ``plan`` is ``None`` when reading or planning failed; a successfully
    planned no-op retains an empty, target-bearing plan.
    ``compiled`` is populated on dry and real runs so the accepted plan and its
    statements remain inspectable even when execution is skipped or blocked.
    ``blocked_failures`` is the derived consequence of other tables' fates,
    baked in at assembly — a blocked table records no execution outcome of its
    own.
    ``diff`` is the complete set of differences the engine found — actions and
    unresolvable differences alike — retained so a table whose plan was
    rejected can still show what drifted. It is ``None`` when the read failed,
    and may be ``None`` on a hand-constructed report.
    """

    read: ReadResult
    planning: PlanningResult | None
    compiled: CompiledPlan | None
    resolution: TableResolution
    execution: ExecutionSummary | None
    blocked_failures: Sequence[ForeignKeyFailure] = ()
    diff: TableDiff | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "blocked_failures", tuple(self.blocked_failures))
        read_failed = isinstance(self.read, ReadFailure)
        planning_failed = isinstance(self.planning, PlanningFailed)
        resolution_failed = bool(self.resolution.structural_failures)

        if read_failed and self.planning is not None:
            raise ValueError("Planning cannot follow a failed read")
        if not read_failed and self.planning is None:
            raise ValueError("A successful read requires a planning outcome")
        if read_failed and self.diff is not None:
            raise ValueError("A failed read produces no diff")

        plan = self.plan
        if plan is not None and plan.target != self.qualified_name:
            raise ValueError("Planned action target must match the reported table")
        if plan is None and self.compiled is not None:
            raise ValueError("Compilation requires a successful planning outcome")
        if plan is not None and self.compiled is None:
            raise ValueError("A successful planning outcome requires compilation")
        if self.compiled is not None and self.compiled.plan != plan:
            raise ValueError("Compiled plan must match the successful planning outcome")
        if self.execution is not None and (read_failed or planning_failed or resolution_failed):
            raise ValueError("Execution cannot follow a failed earlier phase")
        if self.execution is not None and self.execution.compiled_plan != self.compiled:
            raise ValueError("Execution must refer to the reported compiled plan")
        if self.blocked_failures:
            if self.execution is not None:
                raise ValueError("A blocked table records no execution outcome")
            if any(
                failure.reason is not ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
                for failure in self.blocked_failures
            ):
                raise ValueError("Blocked failures must carry the dependency-blocking reason")

    @property
    def plan(self) -> ActionPlan | None:
        """The accepted plan, or ``None`` when reading or planning failed."""
        return accepted_plan(self.planning)

    @property
    def failures(self) -> tuple[Failure, ...]:
        """
        Flatten canonical phase outcomes for callers.

        Lifecycle order: structural, read, planning, execution — derived
        blocking sits at the execution position, where the run it replaced
        would have been.
        """
        failures: list[Failure] = []

        failures.extend(self.resolution.structural_failures)
        if isinstance(self.read, ReadFailure):
            failures.append(self.read)
        if isinstance(self.planning, PlanningFailed):
            failures.extend(self.planning.failures)

        if self.execution is not None:
            failures.extend(self.execution.failures)
        failures.extend(self.blocked_failures)

        return tuple(failures)

    @property
    def desired(self) -> DesiredTable:
        """The declaration this run reconciled, as retained by its resolution."""
        return self.resolution.desired

    @property
    def qualified_name(self) -> QualifiedName:
        """The table identity from the declaration retained by this report."""
        return self.resolution.qualified_name

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

    @property
    def creates_table(self) -> bool:
        """True when the plan brings the table into existence rather than altering it."""
        return any(isinstance(action, CreateTable) for action in self.plan or ())

    @property
    def statement_progress(self) -> StatementProgress | None:
        """How far execution got, or ``None`` when it did not run."""
        if self.execution is None:
            return None
        return StatementProgress(
            applied=self.execution.applied_count,
            planned=len(self.execution.compiled_plan.statements),
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Project this table's run as plain, JSON-serialisable data.

        The field names are a public stability contract (see the run report
        reference doc); changing them is a breaking change.
        """
        progress = self.statement_progress
        execution_record = (
            None if progress is None else {"applied": progress.applied, "total": progress.planned}
        )
        return {
            "name": str(self.qualified_name),
            "status": self.status.value,
            "has_changes": self.has_changes,
            "has_failures": self.has_failures,
            "changes": _change_records(self.plan),
            "rejected_changes": _rejected_change_records(self.diff, self.plan),
            "planned_sql_statements": list(
                self.compiled.statements if self.compiled is not None else ()
            ),
            "failures": _failure_records(self.failures),
            "execution": execution_record,
        }


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Aggregate report for a run across all tables."""

    started_at: datetime
    ended_at: datetime
    table_reports: Sequence[TableRunReport]
    dry_run: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "table_reports", tuple(self.table_reports))

    @classmethod
    def assemble(
        cls,
        *,
        started_at: datetime,
        ended_at: datetime,
        table_reports: Sequence[TableRunReport],
        dry_run: bool,
    ) -> "SyncReport":
        """
        Assemble the run report, deriving dependency blocking from the graph.

        Folds the blocking rule over the reports in dependency order: a table
        that did not converge — own failures, or a dependency that did not —
        marks its name, and a sound table its resolution reports as blocked is
        replaced by a copy carrying those failures. The run recorded nothing
        about blocking; this projection is where the consequence becomes
        visible, dry and real runs alike.
        """
        not_converged: set[QualifiedName] = set()
        derived: list[TableRunReport] = []
        for report in table_reports:
            if report.has_failures:
                not_converged.add(report.qualified_name)
                derived.append(report)
                continue
            blocking = report.resolution.blocked_by(not_converged)
            if blocking:
                not_converged.add(report.qualified_name)
            derived.append(replace(report, blocked_failures=blocking) if blocking else report)
        return cls(
            started_at=started_at,
            ended_at=ended_at,
            table_reports=derived,
            dry_run=dry_run,
        )

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
    def duration_seconds(self) -> float:
        """Wall-clock seconds the run took, start to end."""
        return (self.ended_at - self.started_at).total_seconds()

    @property
    def counts(self) -> RunCounts:
        """
        Per-outcome table counts.

        A table that failed counts as failed whatever else it planned: the
        planned changes were not applied, so reporting them as changes would
        overstate what the run achieved.
        """
        changed = unchanged = failed = 0
        for table_report in self.table_reports:
            if table_report.has_failures:
                failed += 1
            elif table_report.has_changes:
                changed += 1
            else:
                unchanged += 1
        return RunCounts(changed=changed, unchanged=unchanged, failed=failed)

    @property
    def planned_sql_statements(self) -> dict[str, tuple[str, ...]]:
        """Dotted table name → the SQL its plan compiles to; no-op tables omitted."""
        return {
            str(table_report.qualified_name): table_report.compiled.statements
            for table_report in self.table_reports
            if table_report.compiled is not None and table_report.compiled.statements
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
