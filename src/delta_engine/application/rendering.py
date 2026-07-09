"""
Diff and grid rendering for table and sync run reports.

The report value types in report.py are pure data; all human-readable
formatting lives here. The public entry points are render_report (status
grid plus summary footer) and render_diff (per-table change blocks);
render_grid, render_diff_block, run_summary_footer, and _action_entries
are the building blocks they compose.
"""

from collections import Counter
from dataclasses import dataclass
from enum import IntEnum
import functools

from delta_engine.application.ports import ReadFailed
from delta_engine.application.report import SyncReport, TableRunReport
from delta_engine.domain.model import Column, DataType
from delta_engine.domain.plan import (
    Action,
    ActionPlan,
    AddColumn,
    AlterClustering,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
)


def _type_name(data_type: DataType) -> str:
    """Backend-agnostic display name for a domain data type (e.g. 'String')."""
    return type(data_type).__name__


class DiffCategory(IntEnum):
    """Diff line groups, in display order (enum value = order)."""

    COLUMNS = 1
    KEYS = 2
    CLUSTERING = 3
    PROPERTIES = 4
    TAGS = 5
    COMMENTS = 6


# (singular, plural) nouns per category: the plural names the diff group heading;
# the singular is used in the grid's humanized detail count.
_CATEGORY_NOUN: dict[DiffCategory, tuple[str, str]] = {
    DiffCategory.COLUMNS: ("column", "columns"),
    DiffCategory.KEYS: ("key", "keys"),
    DiffCategory.CLUSTERING: ("clustering", "clustering"),
    DiffCategory.PROPERTIES: ("property", "properties"),
    DiffCategory.TAGS: ("tag", "tags"),
    DiffCategory.COMMENTS: ("comment", "comments"),
}


@dataclass(frozen=True, slots=True)
class DiffEntry:
    """One rendered diff line: its category, +/-/~ symbol, and aligned cells."""

    category: DiffCategory
    symbol: str
    cells: tuple[str, ...]


def _column_add_entry(column: Column) -> DiffEntry:
    """Build a '+' columns entry for a created column (name, type, optional NOT NULL)."""
    cells = [column.name, _type_name(column.data_type)]
    if not column.nullable:
        cells.append("NOT NULL")
    return DiffEntry(DiffCategory.COLUMNS, "+", tuple(cells))


@functools.singledispatch
def _action_entries(action: Action) -> tuple[DiffEntry, ...]:
    """Render one plan action as one or more category-tagged diff entries."""
    raise NotImplementedError(f"No diff entries for action {type(action).__name__}")


@_action_entries.register
def _(action: CreateTable) -> tuple[DiffEntry, ...]:
    entries = [_column_add_entry(column) for column in action.table.columns]
    primary_key_columns = action.table.primary_key_columns
    if primary_key_columns:
        entries.append(
            DiffEntry(DiffCategory.KEYS, "+", (f"primary key ({', '.join(primary_key_columns)})",))
        )
    if action.table.clustered_by:
        columns = ", ".join(action.table.clustered_by)
        entries.append(DiffEntry(DiffCategory.CLUSTERING, "+", (f"clustering ({columns})",)))
    return tuple(entries)


@_action_entries.register
def _(action: AddColumn) -> tuple[DiffEntry, ...]:
    return (_column_add_entry(action.column),)


@_action_entries.register
def _(action: DropColumn) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.COLUMNS, "-", (action.column_name,)),)


@_action_entries.register
def _(action: SetColumnNullability) -> tuple[DiffEntry, ...]:
    change = "drop NOT NULL (was NOT NULL)" if action.nullable else "set NOT NULL (was nullable)"
    return (DiffEntry(DiffCategory.COLUMNS, "~", (action.column_name, change)),)


@_action_entries.register
def _(action: SetColumnComment) -> tuple[DiffEntry, ...]:
    if action.comment:
        text = f"column {action.column_name}: '{action.comment}'"
    else:
        text = f"column {action.column_name} comment (unset)"
    return (DiffEntry(DiffCategory.COMMENTS, "~", (text,)),)


@_action_entries.register
def _(action: SetTableComment) -> tuple[DiffEntry, ...]:
    text = f"table: '{action.comment}'" if action.comment else "table comment (unset)"
    return (DiffEntry(DiffCategory.COMMENTS, "~", (text,)),)


@_action_entries.register
def _(action: SetProperty) -> tuple[DiffEntry, ...]:
    if action.observed_value is None:
        return (DiffEntry(DiffCategory.PROPERTIES, "+", (f"{action.name} = '{action.value}'",)),)
    text = f"{action.name} = '{action.value}' (was '{action.observed_value}')"
    return (DiffEntry(DiffCategory.PROPERTIES, "~", (text,)),)


@_action_entries.register
def _(action: UnsetProperty) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.PROPERTIES, "-", (action.name,)),)


@_action_entries.register
def _(action: SetTableTag) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.TAGS, "~", (f"{action.name} = '{action.value}'",)),)


@_action_entries.register
def _(action: UnsetTableTag) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.TAGS, "-", (action.name,)),)


@_action_entries.register
def _(action: SetColumnTag) -> tuple[DiffEntry, ...]:
    text = f"column {action.column_name}.{action.name} = '{action.value}'"
    return (DiffEntry(DiffCategory.TAGS, "~", (text,)),)


@_action_entries.register
def _(action: UnsetColumnTag) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.TAGS, "-", (f"column {action.column_name}.{action.name}",)),)


@_action_entries.register
def _(action: SetPrimaryKey) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.KEYS, "+", (f"primary key ({', '.join(action.columns)})",)),)


@_action_entries.register
def _(action: DropPrimaryKey) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.KEYS, "-", ("primary key",)),)


@_action_entries.register
def _(action: SetForeignKey) -> tuple[DiffEntry, ...]:
    text = f"foreign key ({', '.join(action.local_columns)}) → {action.referenced_table}"
    return (DiffEntry(DiffCategory.KEYS, "+", (text,)),)


@_action_entries.register
def _(action: DropForeignKey) -> tuple[DiffEntry, ...]:
    return (DiffEntry(DiffCategory.KEYS, "-", (f"foreign key {action.constraint_name}",)),)


_OPTIMIZE_FULL_HINT = "run OPTIMIZE FULL to recluster existing data"


@_action_entries.register
def _(action: AlterClustering) -> tuple[DiffEntry, ...]:
    if not action.columns:
        text = f"clustering — {_OPTIMIZE_FULL_HINT}"
        return (DiffEntry(DiffCategory.CLUSTERING, "-", (text,)),)
    columns = ", ".join(action.columns)
    text = f"clustering ({columns}) — {_OPTIMIZE_FULL_HINT}"
    return (DiffEntry(DiffCategory.CLUSTERING, "~", (text,)),)


# Shown wherever a report has a readable state but no planned actions. One
# spelling, two presentations: bare in the grid's DETAIL cell, parenthesised as
# a standalone line in the diff block.
_NO_CHANGES = "no changes"


def _plan_creates_table(plan: ActionPlan) -> bool:
    return any(isinstance(action, CreateTable) for action in plan)


def _render_entry_groups(entries: list[DiffEntry]) -> list[str]:
    """Group entries by category (display order) and align cells within each group."""
    lines: list[str] = []
    for category in DiffCategory:
        group = [entry for entry in entries if entry.category is category]
        if not group:
            continue
        lines.append(f"  {_CATEGORY_NOUN[category][1]}")
        widths: dict[int, int] = {}
        for entry in group:
            for index, cell in enumerate(entry.cells):
                widths[index] = max(widths.get(index, 0), len(cell))
        for entry in group:
            padded = "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(entry.cells))
            lines.append(f"    {entry.symbol} {padded}".rstrip())
    return lines


def render_diff_block(report: TableRunReport) -> str:
    """Render one table's change block: its name then its planned changes, grouped."""
    header = str(report.qualified_name)
    if isinstance(report.read, ReadFailed):
        return f"{header}\n  (could not read — no diff)"
    if not report.plan:
        if report.has_failures:
            return f"{header}\n  ({_NO_CHANGES} — see failures)"
        return f"{header}\n  ({_NO_CHANGES})"
    if _plan_creates_table(report.plan):
        header = f"{header}  (CREATE)"
    entries = [entry for action in report.plan for entry in _action_entries(action)]
    return "\n".join([header, *_render_entry_groups(entries)])


_DETAIL_MAX_CHARS = 60

_GRID_HEADERS = ("TABLE", "STATUS", "ACTIONS", "DETAIL")


def _grid_actions_cell(report: TableRunReport) -> str:
    """ACTIONS cell: applied/total when execution ran, — on a plan-less failure, else count."""
    execution = report.execution
    if execution is not None:
        applied = len(execution.results) - execution.failed_count
        return f"{applied}/{len(report.plan)}"
    if report.has_failures:
        return "—"
    return str(len(report.plan))


def _humanized_action_summary(plan: ActionPlan) -> str:
    """Summarise a plan as per-category change counts in display order, e.g. '2 columns, 1 key'."""
    counts: Counter[DiffCategory] = Counter()
    for action in plan:
        for entry in _action_entries(action):
            counts[entry.category] += 1
    parts: list[str] = []
    for category in DiffCategory:
        count = counts.get(category, 0)
        if count:
            singular, plural = _CATEGORY_NOUN[category]
            parts.append(f"{count} {singular if count == 1 else plural}")
    return ", ".join(parts) or _NO_CHANGES


def _grid_detail(report: TableRunReport) -> str:
    """Return the DETAIL cell: first failure headline, or a per-category change count."""
    if report.has_failures:
        failures = report.failures
        first = failures[0].headline()
        extra = len(failures) - 1
        return f"{first} (+{extra} more)" if extra else first
    return _humanized_action_summary(report.plan)


def _truncate(text: str, limit: int = _DETAIL_MAX_CHARS) -> str:
    """Truncate with an ellipsis when longer than ``limit``."""
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _grid_row_cells(report: TableRunReport) -> tuple[str, str, str, str]:
    """Return the four grid cells for one report (DETAIL already truncated)."""
    return (
        str(report.qualified_name),
        report.status.value,
        _grid_actions_cell(report),
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
        elif table_report.plan:
            changed += 1
        else:
            unchanged += 1
    seconds = (report.ended_at - report.started_at).total_seconds()
    total = len(report.table_reports)
    return (
        f"{total} tables: {changed} changed, {unchanged} unchanged, "
        f"{failed} failed ({seconds:.1f}s)"
    )


def _heading(text: str, rule: str = "=") -> str:
    """Render a section title underlined with a rule the width of the text."""
    return f"{text}\n{rule * len(text)}"


def _dry_run_banner(report: SyncReport) -> str:
    """Return the dry-run banner, or empty for an applied run."""
    return "DRY RUN — no changes applied" if report.dry_run else ""


def render_failures_section(reports: tuple[TableRunReport, ...]) -> str:
    """Render full per-table failure detail for every failed table; empty when none failed."""
    failed = [report for report in reports if report.has_failures]
    if not failed:
        return ""
    blocks: list[str] = []
    for report in failed:
        lines = [f"  {report.qualified_name}"]
        for failure in report.failures:
            lines.extend(f"    {line}" for line in failure.format_lines())
        blocks.append("\n".join(lines))
    return "\n".join([_heading("Failures", rule="-"), *blocks])


def render_report(report: SyncReport) -> str:
    """Render the run: title, optional dry-run banner, status grid, failures section, footer."""
    parts = (
        _heading("SYNC REPORT"),
        _dry_run_banner(report),
        render_grid(report.table_reports),
        render_failures_section(report.table_reports),
        run_summary_footer(report),
    )
    return "\n\n".join(part for part in parts if part)


def render_diff(report: SyncReport) -> str:
    """Render every table's planned changes as +/-/~ blocks, under a DIFF title."""
    blocks = [render_diff_block(table_report) for table_report in report.table_reports]
    return "\n\n".join([_heading("DIFF"), *blocks])
