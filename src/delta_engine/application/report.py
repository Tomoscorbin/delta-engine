"""
Run records: per-table and run-level outcome aggregates.

`TableRun` is the immutable record of one table's sync run, born complete
at the engine's plan pass and enriched afterwards with the cross-table
facts (execution, dependency blocking); `SyncReport` aggregates those
records for the whole run.
"""

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, NamedTuple, Self

from delta_engine.application.diff_entries import DiffEntry, drift_entries, plan_entries
from delta_engine.application.failures import (
    ExecutionFailure,
    Failure,
    FailurePhase,
    ForeignKeyFailure,
    ForeignKeyFailureReason,
    ReadFailure,
    ValidationFailure,
)
from delta_engine.application.planning import (
    PlanningDeferred,
    PlanningRejected,
    PlanningResult,
    accepted_plan,
)
from delta_engine.application.ports import CompiledPlan, ExecutionResult, ReadResult
from delta_engine.application.relationships import TableResolution
from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model import DesiredTable, QualifiedName
from delta_engine.domain.plan import ActionPlan, CreateTable, TableDiff, TableDrift

# The versioned wire format `to_dict` emits; additive keys do not bump it.
_SCHEMA_VERSION: Final = 2

# ---------- Status enums ----------


class TableRunStatus(StrEnum):
    """High-level status of a table's sync run."""

    SUCCESS = "SUCCESS"
    DEFERRED = "DEFERRED"
    READ_FAILED = "READ_FAILED"
    PLANNING_FAILED = "PLANNING_FAILED"
    FOREIGN_KEY_FAILED = "FOREIGN_KEY_FAILED"
    EXECUTION_FAILED = "EXECUTION_FAILED"


class TableChangeState(StrEnum):
    """What happened to one table's intended catalog change within a run."""

    NOT_PLANNED = "not planned"
    DEFERRED = "deferred"
    UNCHANGED = "unchanged"
    PLANNED = "planned"
    NOT_APPLIED = "not applied"
    PARTIALLY_APPLIED = "partially applied"
    APPLIED = "applied"


# ---------- Derived run facts ----------


class StatementProgress(NamedTuple):
    """How many of a table's planned statements execution actually applied."""

    applied: int
    planned: int


class RunCounts(NamedTuple):
    """
    How a run's tables came out.

    Every table falls in exactly one bucket, so ``total`` is their sum rather
    than a separately counted fact that could disagree with them. ``deferred``
    counts the tables that do not exist and whose declarations cannot create
    them — waiting, neither changed nor failed.
    """

    changed: int
    unchanged: int
    failed: int
    deferred: int = 0

    @property
    def total(self) -> int:
        """Tables in the run."""
        return self.changed + self.unchanged + self.failed + self.deferred


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
            "detail": " ".join(phrase for phrase in entry.detail if phrase),
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


def _rejected_change_records(planning: PlanningResult | None) -> list[dict[str, str]]:
    """
    Summarise the differences a rejected diff found, in diff order.

    Empty for an accepted outcome: an accepted diff's differences are its
    changes, already projected by ``_change_records``.
    """
    if not isinstance(planning, PlanningRejected) or not isinstance(planning.diff, TableDrift):
        return []
    return _entry_records(drift_entries(planning.diff))


def _failure_facts(failure: Failure) -> dict[str, Any]:
    """
    Return the variant's own lossless facts, added beside the rendered ``message``.

    ``message`` may truncate a long backend message for display;
    ``diagnostic`` carries the complete text for the variants that have one.
    """
    match failure:
        case ReadFailure():
            return {
                "exception_type": failure.exception_type,
                "diagnostic": failure.message,
            }
        case ExecutionFailure():
            return {
                "exception_type": failure.exception_type,
                "diagnostic": failure.message,
                "statement_index": failure.statement_index,
                "sql": failure.statement,
            }
        case ValidationFailure():
            return {
                "rule": failure.rule_name,
                "subject": failure.subject,
                "details": list(failure.details),
            }
        case ForeignKeyFailure():
            return {
                "reason": failure.reason.value,
                "columns": list(failure.local_columns),
                "references": str(failure.references),
            }
        case _:
            raise NotImplementedError(f"No wire facts for failure {type(failure).__name__}")


def _failure_records(failures: tuple[Failure, ...]) -> list[dict[str, Any]]:
    """Project failures as flat records, in phase order as carried."""
    return [
        {
            "phase": failure.phase.name,
            "type": type(failure).__name__,
            "message": " ".join(failure.format_lines()),
            **_failure_facts(failure),
        }
        for failure in failures
    ]


@dataclass(frozen=True, slots=True)
class TableRun:
    """
    Frozen public record of one table's sync run.

    Born complete at the engine's plan pass: everything the table can know
    alone — its resolution, read, planning outcome, and compiled SQL — is
    fixed at construction, in lifecycle field order, and the trailing fields
    default to their not-applicable state. The two facts that depend on other
    tables are attached afterwards as functional updates: ``execution`` by
    the execute walk, ``blocked_failures`` at assembly. A run without
    execution is a legitimate terminal state (a dry run, a blocked table, a
    no-op plan, a deferred absent table), not an unfinished one.

    ``plan`` is ``None`` when reading or planning failed; a successfully
    planned no-op retains an empty, target-bearing plan.
    ``compiled`` is populated on dry and real runs so the accepted plan and its
    statements remain inspectable even when execution is skipped or blocked.
    ``blocked_failures`` is the derived consequence of other tables' fates,
    baked in at assembly — a blocked table records no execution outcome of its
    own.
    ``diff`` is derived from the planning outcome, which retains the complete
    set of differences it planned from — actions and unresolvable differences
    alike — so a table whose plan was rejected can still show what drifted. It
    is ``None`` when the read failed, because planning never ran.
    """

    resolution: TableResolution
    read: ReadResult
    planning: PlanningResult | None = None
    compiled: CompiledPlan | None = None
    execution: ExecutionResult | None = None
    blocked_failures: ListOrTuple[ForeignKeyFailure] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "blocked_failures", tuple(self.blocked_failures))
        read_failed = isinstance(self.read, ReadFailure)
        planning_failed = isinstance(self.planning, PlanningRejected)
        resolution_failed = bool(self.resolution.structural_failures)

        if read_failed and self.planning is not None:
            raise ValueError("Planning cannot follow a failed read")
        if not read_failed and self.planning is None:
            raise ValueError("A successful read requires a planning outcome")
        if self.planning is not None and self.planning.diff.target != self.qualified_name:
            raise ValueError("Planning outcome must target the reported table")

        plan = self.plan
        if plan is None and self.compiled is not None:
            raise ValueError("Compilation requires a successful planning outcome")
        if plan is not None and self.compiled is None:
            raise ValueError("A successful planning outcome requires compilation")
        if self.compiled is not None and self.compiled.plan != plan:
            raise ValueError("Compiled plan must match the successful planning outcome")
        if self.execution is not None and plan is None:
            raise ValueError("Execution requires a successful planning outcome")
        if self.execution is not None and resolution_failed:
            raise ValueError("Execution cannot follow a failed earlier phase")
        if self.execution is not None and self.execution.compiled_plan != self.compiled:
            raise ValueError("Execution must refer to the reported compiled plan")
        if self.blocked_failures:
            if self.execution is not None:
                raise ValueError("A blocked table records no execution outcome")
            if read_failed or planning_failed or resolution_failed:
                raise ValueError("A blocked table cannot also carry its own failures")
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
    def diff(self) -> TableDiff | None:
        """The diff the planning outcome retains, or ``None`` when the read failed."""
        return None if self.planning is None else self.planning.diff

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
        if isinstance(self.planning, PlanningRejected):
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
        """
        Status of the earliest phase that failed; DEFERRED or SUCCESS when none did.

        Failures dominate deferral: a deferred table that also failed
        resolution reports the failure, because that is what needs acting on.
        """
        if self.failures:
            return _STATUS_FOR_PHASE[min(failure.phase for failure in self.failures)]
        if isinstance(self.planning, PlanningDeferred):
            return TableRunStatus.DEFERRED
        return TableRunStatus.SUCCESS

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
            "rejected_changes": _rejected_change_records(self.planning),
            "planned_sql_statements": list(
                self.compiled.statements if self.compiled is not None else ()
            ),
            "failures": _failure_records(self.failures),
            "execution": execution_record,
        }


def _table_change_state(run: TableRun, *, dry_run: bool) -> TableChangeState:
    """Derive what happened to a table's intended catalog change."""
    if run.status is TableRunStatus.DEFERRED:
        return TableChangeState.DEFERRED
    plan = run.plan
    if plan is None:
        return TableChangeState.NOT_PLANNED
    if not plan:
        return TableChangeState.UNCHANGED
    if dry_run:
        return TableChangeState.PLANNED

    execution = run.execution
    if execution is None:
        return TableChangeState.NOT_APPLIED
    if not execution.failures:
        return TableChangeState.APPLIED
    if execution.applied_count == 0:
        return TableChangeState.NOT_APPLIED
    return TableChangeState.PARTIALLY_APPLIED


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Aggregate report for a run across all tables."""

    started_at: datetime
    ended_at: datetime
    table_runs: ListOrTuple[TableRun]
    dry_run: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "table_runs", tuple(self.table_runs))
        if self.dry_run and any(run.execution is not None for run in self.table_runs):
            raise ValueError("A dry run cannot contain execution results")
        if not self.dry_run and any(
            run.has_changes and run.execution is None and not run.has_failures
            for run in self.table_runs
        ):
            raise ValueError(
                "A real run with a non-empty plan requires execution or a failure explaining"
                " why execution did not run"
            )

    @classmethod
    def assemble(
        cls,
        *,
        started_at: datetime,
        ended_at: datetime,
        table_runs: ListOrTuple[TableRun],
        dry_run: bool,
    ) -> Self:
        """
        Assemble the run report, deriving dependency blocking from the graph.

        Folds the blocking rule over the runs in dependency order: a table
        that did not converge — own failures, or a dependency that did not —
        marks its name, and a sound table its resolution reports as blocked is
        replaced by a copy carrying those failures. The run recorded nothing
        about blocking; this projection is where the consequence becomes
        visible, dry and real runs alike.
        """
        not_converged: set[QualifiedName] = set()
        derived: list[TableRun] = []
        for run in table_runs:
            if run.has_failures:
                not_converged.add(run.qualified_name)
                derived.append(run)
                continue
            blocking = run.resolution.blocked_by(not_converged)
            if blocking:
                not_converged.add(run.qualified_name)
            derived.append(replace(run, blocked_failures=blocking) if blocking else run)
        return cls(
            started_at=started_at,
            ended_at=ended_at,
            table_runs=derived,
            dry_run=dry_run,
        )

    @property
    def has_failures(self) -> bool:
        """Return True if any table failed in the run."""
        return any(run.has_failures for run in self.table_runs)

    @property
    def has_changes(self) -> bool:
        """
        True if any table's plan holds actions.

        States facts about *planned* changes only: a table that failed
        validation contributes failures, not changes. The CI gate idiom is
        ``report.has_failures or report.has_changes``.
        """
        return any(run.has_changes for run in self.table_runs)

    @property
    def table_change_states(self) -> tuple[TableChangeState, ...]:
        """Catalog change state for each table run, in run order."""
        return tuple(_table_change_state(run, dry_run=self.dry_run) for run in self.table_runs)

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
        changed = unchanged = failed = deferred = 0
        for run in self.table_runs:
            if run.has_failures:
                failed += 1
            elif run.status is TableRunStatus.DEFERRED:
                deferred += 1
            elif run.has_changes:
                changed += 1
            else:
                unchanged += 1
        return RunCounts(changed=changed, unchanged=unchanged, failed=failed, deferred=deferred)

    @property
    def planned_sql_statements(self) -> dict[str, tuple[str, ...]]:
        """Dotted table name → the SQL its plan compiles to; no-op tables omitted."""
        return {
            str(run.qualified_name): tuple(run.compiled.statements)
            for run in self.table_runs
            if run.compiled is not None and run.compiled.statements
        }

    @property
    def failures_by_table(self) -> dict[QualifiedName, tuple[Failure, ...]]:
        """Mapping of qualified table name to its failures (if any)."""
        return {run.qualified_name: run.failures for run in self.table_runs if run.has_failures}

    def render(self) -> str:
        """Render the run's status, failures, and summary as human-readable text."""
        # rendering imports these report types, so defer the reverse dependency
        # until callers ask for the convenience view.
        from delta_engine.application.rendering import render_report

        return render_report(self)

    def render_diff(self) -> str:
        """Render every table's planned changes as human-readable text."""
        from delta_engine.application.rendering import render_diff

        return render_diff(self)

    def to_dict(self) -> dict[str, Any]:
        """Project the whole run as plain, JSON-serialisable data; tables in run order."""
        return {
            "schema_version": _SCHEMA_VERSION,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "dry_run": self.dry_run,
            "has_changes": self.has_changes,
            "has_failures": self.has_failures,
            "tables": [run.to_dict() for run in self.table_runs],
        }

    def __iter__(self) -> Iterator[TableRun]:
        return iter(self.table_runs)
