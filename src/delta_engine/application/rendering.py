"""
Diff and grid rendering for table and sync run reports.

Separates display logic from the result/report value types in results.py.
The three public entry points are render_diff_block, render_grid, and
run_summary_footer; action_diff_line handles per-action diff lines via
singledispatch.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING

from delta_engine.domain.plan.actions import (
    Action,
    AddColumn,
    ColumnTypeChange,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    PartitioningChange,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetTableTag,
)

if TYPE_CHECKING:
    from delta_engine.application.results import SyncReport, TableRunReport


def _type_name(data_type: object) -> str:
    """Backend-agnostic display name for a domain data type (e.g. 'String')."""
    return type(data_type).__name__


@functools.singledispatch
def action_diff_line(action: Action) -> str:
    """Render one plan action as a single +/-/~ diff line."""
    raise NotImplementedError(f"No diff line for action {type(action).__name__}")


@action_diff_line.register
def _(action: CreateTable) -> str:
    columns = ", ".join(column.name for column in action.table.columns)
    return f"+ create table (columns: {columns})"


@action_diff_line.register
def _(action: AddColumn) -> str:
    nullable = "" if action.column.nullable else " NOT NULL"
    return f"+ column {action.column.name} {_type_name(action.column.data_type)}{nullable}"


@action_diff_line.register
def _(action: DropColumn) -> str:
    return f"- column {action.column_name}"


@action_diff_line.register
def _(action: SetColumnNullability) -> str:
    direction = "drop" if action.nullable else "set"
    return f"~ column {action.column_name} {direction} NOT NULL"


@action_diff_line.register
def _(action: SetColumnComment) -> str:
    suffix = "" if action.comment else " (unset)"
    return f"~ comment on column {action.column_name}{suffix}"


@action_diff_line.register
def _(action: SetTableComment) -> str:
    return "~ comment on table"


@action_diff_line.register
def _(action: SetProperty) -> str:
    return f"~ property {action.name} = '{action.value}'"


@action_diff_line.register
def _(action: SetTableTag) -> str:
    return f"~ tag {action.name} = '{action.value}'"


@action_diff_line.register
def _(action: UnsetTableTag) -> str:
    return f"- tag {action.name}"


@action_diff_line.register
def _(action: SetColumnTag) -> str:
    return f"~ column tag {action.column_name}.{action.name} = '{action.value}'"


@action_diff_line.register
def _(action: UnsetColumnTag) -> str:
    return f"- column tag {action.column_name}.{action.name}"


@action_diff_line.register
def _(action: SetPrimaryKey) -> str:
    columns = ", ".join(action.columns)
    return f"+ primary key ({columns})"


@action_diff_line.register
def _(action: DropPrimaryKey) -> str:
    return "- primary key"


@action_diff_line.register
def _(action: SetForeignKey) -> str:
    columns = ", ".join(action.foreign_key.local_columns)
    return f"+ foreign key ({columns}) → {action.foreign_key.references}"


@action_diff_line.register
def _(action: DropForeignKey) -> str:
    return f"- foreign key {action.constraint_name}"


@action_diff_line.register
def _(action: ColumnTypeChange) -> str:
    return (
        f"~ column {action.column_name} type "
        f"{_type_name(action.from_type)} → {_type_name(action.to_type)}"
    )


@action_diff_line.register
def _(action: PartitioningChange) -> str:
    return f"~ partitioning {action.observed_partitioning} → {action.desired_partitioning}"


def render_diff_block(report: TableRunReport) -> str:
    """Render one table's change block: its name then one line per planned action."""
    from delta_engine.application.results import ReadFailed

    header = str(report.qualified_name)
    if isinstance(report.read, ReadFailed):
        return f"{header}\n  (could not read — no diff)"
    if not report.plan:
        return f"{header}\n  (no changes)"
    lines = [f"  {action_diff_line(action)}" for action in report.plan]
    return "\n".join([header, *lines])


_DETAIL_MAX_CHARS = 60

_GRID_HEADERS = ("TABLE", "STATUS", "ACTIONS", "DETAIL")


def _grid_detail(report: TableRunReport) -> str:
    """Return the DETAIL cell: first failure summary, action names, or 'no changes'."""
    if report.has_failures:
        failures = report.failures
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


def render_grid(reports: tuple[TableRunReport, ...]) -> str:
    """Render an aligned TABLE | STATUS | ACTIONS | DETAIL grid for ``reports``."""
    rows = [_GRID_HEADERS, *(_grid_row_cells(report) for report in reports)]
    widths = [max(len(row[col]) for row in rows) for col in range(len(_GRID_HEADERS))]
    return "\n".join(
        "  ".join(cell.ljust(widths[col]) for col, cell in enumerate(row)).rstrip() for row in rows
    )


def run_summary_footer(report: SyncReport) -> str:
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
