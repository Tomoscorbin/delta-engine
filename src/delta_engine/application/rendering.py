"""
Diff and grid rendering for table and sync run reports.

The report value types in report.py are pure data; all human-readable
formatting lives here. Action interpretation (what each action means) lives
in diff_entries.py, the shared meaning layer this module and to_dict()
consume. The public entry points are render_report (status grid plus summary
footer) and render_diff (per-table change blocks); render_grid,
render_diff_block, and run_summary_footer are the building blocks they compose.
"""

from collections import Counter
from typing import Final

from delta_engine.application.diff_entries import (
    CATEGORY_NOUN,
    DiffCategory,
    DiffEntry,
    action_entries,
)
from delta_engine.application.ports import ReadFailed
from delta_engine.application.report import SyncReport, TableRunReport
from delta_engine.domain.plan import ActionPlan, CreateTable

# Shown wherever a report has a readable state but no planned actions. One
# spelling, two presentations: bare in the grid's DETAIL cell, parenthesised as
# a standalone line in the diff block.
_NO_CHANGES: Final[str] = "no changes"

_DETAIL_MAX_CHARS: Final[int] = 60

_GRID_HEADERS: Final[tuple[str, str, str, str]] = ("TABLE", "STATUS", "STATEMENTS", "DETAIL")


def _plan_creates_table(plan: ActionPlan) -> bool:
    return any(isinstance(action, CreateTable) for action in plan)


def _render_entry_groups(entries: list[DiffEntry]) -> list[str]:
    """Group entries by category (display order) and align cells within each group."""
    lines: list[str] = []
    for category in DiffCategory:
        group = [entry for entry in entries if entry.category is category]
        if not group:
            continue
        lines.append(f"  {CATEGORY_NOUN[category][1]}")
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
    entries = [entry for action in report.plan for entry in action_entries(action)]
    return "\n".join([header, *_render_entry_groups(entries)])


def _grid_statements_cell(report: TableRunReport) -> str:
    """STATEMENTS cell: applied/planned when execution ran, — on a failure, else planned count."""
    if report.execution is not None:
        return f"{report.execution.applied_count}/{len(report.planned_sql_statements)}"
    if report.has_failures:
        return "—"
    return str(len(report.planned_sql_statements))


def _humanized_action_summary(plan: ActionPlan) -> str:
    """Summarise a plan as per-category change counts in display order, e.g. '2 columns, 1 key'."""
    counts: Counter[DiffCategory] = Counter()
    for action in plan:
        for entry in action_entries(action):
            counts[entry.category] += 1
    parts: list[str] = []
    for category in DiffCategory:
        count = counts.get(category, 0)
        if count:
            singular, plural = CATEGORY_NOUN[category]
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
        _grid_statements_cell(report),
        _truncate(_grid_detail(report)),
    )


def render_grid(reports: tuple[TableRunReport, ...]) -> str:
    """Render an aligned TABLE | STATUS | STATEMENTS | DETAIL grid for ``reports``."""
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
    return "PLAN — no planned SQL executed" if report.dry_run else ""


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
