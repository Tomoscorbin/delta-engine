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
from enum import StrEnum
import functools

from delta_engine.domain.model import ObservedTable, QualifiedName
from delta_engine.domain.plan.actions import (
    Action,
    ActionPlan,
    AddColumn,
    ColumnTypeChange,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    PartitioningChange,
    SetColumnComment,
    SetColumnNullability,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
)

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


_FOREIGN_KEY_REASON_DETAIL: dict[ForeignKeyFailureReason, str] = {
    ForeignKeyFailureReason.CYCLE: "it is part of a foreign key dependency cycle",
    ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE: (
        "it references a table that is not registered"
    ),
    ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY: (
        "it references a table that failed to sync"
    ),
    ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY: (
        "its referenced columns are not the primary key of the referenced table"
    ),
}


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


@dataclass(frozen=True, slots=True)
class ForeignKeyFailure(Failure):
    """A foreign key constraint that could not be applied, failing its whole table."""

    table: QualifiedName
    local_columns: tuple[str, ...]
    references: str
    reason: ForeignKeyFailureReason

    def format_lines(self) -> tuple[str, ...]:
        columns = ", ".join(self.local_columns)
        return (
            f"Foreign key ({columns}) → {self.references} on {self.table} was not applied: "
            f"{_FOREIGN_KEY_REASON_DETAIL[self.reason]}.",
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
    """Per-table report with outcomes and failures."""

    qualified_name: QualifiedName
    read: CatalogState
    plan: ActionPlan = field(default_factory=ActionPlan)
    pre_execution_failures: tuple[Failure, ...] = ()
    execution: ExecutionSummary | None = None

    @property
    def status(self) -> TableRunStatus:
        """Aggregate table status across read, pre-execution, and execution phases."""
        if isinstance(self.read, ReadFailed):
            return TableRunStatus.READ_FAILED
        if any(isinstance(f, ForeignKeyFailure) for f in self.pre_execution_failures):
            return TableRunStatus.FOREIGN_KEY_FAILED
        if any(isinstance(f, ValidationFailure) for f in self.pre_execution_failures):
            return TableRunStatus.VALIDATION_FAILED
        if self.execution is not None and self.execution.failed:
            return TableRunStatus.EXECUTION_FAILED
        return TableRunStatus.SUCCESS

    @property
    def has_failures(self) -> bool:
        """True if the table did not fully succeed."""
        return self.status is not TableRunStatus.SUCCESS

    @property
    def all_failures(self) -> tuple[Failure, ...]:
        """All failures for this table (read, pre-execution, execution)."""
        out: list[Failure] = []
        if isinstance(self.read, ReadFailed):
            out.append(self.read.failure)
        out.extend(self.pre_execution_failures)
        if self.execution is not None:
            out.extend(self.execution.failures)
        return tuple(out)

    def diff(self) -> str:
        """Render this table's planned changes as a +/-/~ change list."""
        return _render_diff_block(self)

    def __str__(self) -> str:
        """Render this report as the grid header plus its single row."""
        return _render_grid((self,))


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
        return {t.qualified_name: t.all_failures for t in self.table_reports if t.has_failures}

    def __iter__(self) -> Iterator[TableRunReport]:
        return iter(self.table_reports)

    def diff(self) -> str:
        """Render every table's planned changes, in report order."""
        return "\n\n".join(_render_diff_block(report) for report in self.table_reports)

    def __str__(self) -> str:
        """Render the run as an aligned grid followed by a summary footer."""
        if not self.table_reports:
            return "Sync report: 0 tables"
        return f"{_render_grid(self.table_reports)}\n\n{_run_summary_footer(self)}"


# ---------- Display


def _type_name(data_type: object) -> str:
    """Backend-agnostic display name for a domain data type (e.g. 'String')."""
    return type(data_type).__name__


@functools.singledispatch
def _action_diff_line(action: Action) -> str:
    """Render one plan action as a single +/-/~ diff line."""
    raise NotImplementedError(f"No diff line for action {type(action).__name__}")


@_action_diff_line.register
def _(action: CreateTable) -> str:
    columns = ", ".join(column.name for column in action.table.columns)
    return f"+ create table (columns: {columns})"


@_action_diff_line.register
def _(action: AddColumn) -> str:
    nullable = "" if action.column.nullable else " NOT NULL"
    return f"+ column {action.column.name} {_type_name(action.column.data_type)}{nullable}"


@_action_diff_line.register
def _(action: DropColumn) -> str:
    return f"- column {action.column_name}"


@_action_diff_line.register
def _(action: SetColumnNullability) -> str:
    direction = "drop" if action.nullable else "set"
    return f"~ column {action.column_name} {direction} NOT NULL"


@_action_diff_line.register
def _(action: SetColumnComment) -> str:
    suffix = "" if action.comment else " (unset)"
    return f"~ comment on column {action.column_name}{suffix}"


@_action_diff_line.register
def _(action: SetTableComment) -> str:
    return "~ comment on table"


@_action_diff_line.register
def _(action: SetProperty) -> str:
    return f"~ property {action.name} = '{action.value}'"


@_action_diff_line.register
def _(action: SetPrimaryKey) -> str:
    columns = ", ".join(column.name for column in action.columns)
    return f"+ primary key ({columns})"


@_action_diff_line.register
def _(action: DropPrimaryKey) -> str:
    return "- primary key"


@_action_diff_line.register
def _(action: SetForeignKey) -> str:
    return f"+ foreign key ({action.subject})"


@_action_diff_line.register
def _(action: DropForeignKey) -> str:
    return f"- foreign key {action.constraint_name}"


@_action_diff_line.register
def _(action: ColumnTypeChange) -> str:
    return (
        f"~ column {action.column_name} type "
        f"{_type_name(action.from_type)} → {_type_name(action.to_type)}"
    )


@_action_diff_line.register
def _(action: PartitioningChange) -> str:
    return f"~ partitioning {action.observed_partitioning} → {action.desired_partitioning}"


def _render_diff_block(report: TableRunReport) -> str:
    """Render one table's change block: its name then one line per planned action."""
    header = str(report.qualified_name)
    if isinstance(report.read, ReadFailed):
        return f"{header}\n  (could not read — no diff)"
    if not report.plan:
        return f"{header}\n  (no changes)"
    lines = [f"  {_action_diff_line(action)}" for action in report.plan]
    return "\n".join([header, *lines])


_DETAIL_MAX_CHARS = 60

_GRID_HEADERS = ("TABLE", "STATUS", "ACTIONS", "DETAIL")


def _grid_detail(report: TableRunReport) -> str:
    """Return the DETAIL cell: first failure summary, action names, or 'no changes'."""
    if report.has_failures:
        failures = report.all_failures
        first = failures[0].format_lines()[0]
        extra = len(failures) - 1
        return f"{first} (+{extra} more)" if extra else first
    if len(report.plan):
        return ", ".join(type(action).__name__ for action in report.plan)
    return "no changes"


def _truncate(text: str, limit: int = _DETAIL_MAX_CHARS) -> str:
    """Truncate with an ellipsis when longer than ``limit``."""
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _grid_row_cells(report: TableRunReport) -> tuple[str, str, str, str]:
    """Return the four grid cells for one report (DETAIL already truncated)."""
    return (
        str(report.qualified_name),
        report.status.value,
        str(len(report.plan)),
        _truncate(_grid_detail(report)),
    )


def _render_grid(reports: tuple[TableRunReport, ...]) -> str:
    """Render an aligned TABLE | STATUS | ACTIONS | DETAIL grid for ``reports``."""
    rows = [_GRID_HEADERS, *(_grid_row_cells(report) for report in reports)]
    widths = [max(len(row[col]) for row in rows) for col in range(len(_GRID_HEADERS))]
    return "\n".join(
        "  ".join(cell.ljust(widths[col]) for col, cell in enumerate(row)).rstrip() for row in rows
    )


def _run_summary_footer(report: SyncReport) -> str:
    """One-line summary: table total, changed/unchanged/failed counts, duration."""
    changed = unchanged = failed = 0
    for table_report in report.table_reports:
        if table_report.has_failures:
            failed += 1
        elif len(table_report.plan):
            changed += 1
        else:
            unchanged += 1
    seconds = (report.ended_at - report.started_at).total_seconds()
    total = len(report.table_reports)
    return (
        f"{total} tables: {changed} changed, {unchanged} unchanged, "
        f"{failed} failed ({seconds:.1f}s)"
    )
