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
from collections.abc import Sequence
from typing import Final

from delta_engine.application.diff_entries import (
    DiffCategory,
    DiffEntry,
    drift_entries,
    plan_entries,
)
from delta_engine.application.failures import ReadFailure
from delta_engine.application.report import SyncReport, TableRunReport
from delta_engine.domain.plan import ActionPlan, TableDrift

# Shown wherever a report has a readable state but no planned actions. One
# spelling, two presentations: bare in the grid's DETAIL cell, parenthesised as
# a standalone line in the diff block.
_NO_CHANGES: Final[str] = "no changes"

_DETAIL_MAX_CHARS: Final[int] = 60

_GRID_HEADERS: Final[tuple[str, str, str, str]] = ("TABLE", "STATUS", "STATEMENTS", "DETAIL")


def _aligned_rows(rows: Sequence[Sequence[str]]) -> list[str]:
    """
    Pad ragged rows into columns, each row joined and right-stripped.

    Rows need not be the same length: a column is as wide as the widest cell
    any row puts there, and rows that stop short simply end early.
    """
    widths: dict[int, int] = {}
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths.get(index, 0), len(cell))
    return [
        "  ".join(cell.ljust(widths[index]) for index, cell in enumerate(row)).rstrip()
        for row in rows
    ]


def _render_entry_groups(entries: Sequence[DiffEntry]) -> list[str]:
    """Group entries by category (display order) and align cells within each group."""
    lines: list[str] = []
    for category in DiffCategory:
        group = [entry for entry in entries if entry.category is category]
        if not group:
            continue
        lines.append(f"  {category.plural}")
        bodies = _aligned_rows([entry.cells for entry in group])
        lines.extend(
            f"    {entry.symbol} {body}".rstrip() for entry, body in zip(group, bodies, strict=True)
        )
    return lines


def render_diff_block(report: TableRunReport) -> str:
    """Render one table's change block: its name then its changes, grouped."""
    header = str(report.qualified_name)
    if isinstance(report.read, ReadFailure):
        return f"{header}\n  (could not read — no diff)"

    plan = report.plan
    if plan is None:
        # No plan means the diff was rejected, which is not the same as having
        # found nothing. The failures section says which rule refused; this
        # says what it refused.
        refused = drift_entries(report.diff) if isinstance(report.diff, TableDrift) else ()
        if refused:
            return "\n".join(
                [f"{header}  (REJECTED — no SQL planned)", *_render_entry_groups(refused)]
            )
        return f"{header}\n  ({_NO_CHANGES} — see failures)"

    if not plan:
        if report.has_failures:
            return f"{header}\n  ({_NO_CHANGES} — see failures)"
        return f"{header}\n  ({_NO_CHANGES})"

    if report.creates_table:
        header = f"{header}  (CREATE)"
    return "\n".join([header, *_render_entry_groups(plan_entries(plan))])


def _grid_statements_cell(report: TableRunReport) -> str:
    """STATEMENTS cell: applied/planned when execution ran, — on a failure, else planned count."""
    progress = report.statement_progress
    if progress is not None:
        return f"{progress.applied}/{progress.planned}"
    if report.has_failures:
        return "—"
    return str(len(report.compiled.statements)) if report.compiled is not None else "0"


def _humanized_action_summary(plan: ActionPlan | None) -> str:
    """Summarise a plan as per-category change counts in display order, e.g. '2 columns, 1 key'."""
    if plan is None:
        return _NO_CHANGES
    counts = Counter(entry.category for entry in plan_entries(plan))
    parts = [category.counted(counts[category]) for category in DiffCategory if counts[category]]
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


def render_grid(reports: Sequence[TableRunReport]) -> str:
    """Render an aligned TABLE | STATUS | STATEMENTS | DETAIL grid for ``reports``."""
    rows = [_GRID_HEADERS, *(_grid_row_cells(report) for report in reports)]
    return "\n".join(_aligned_rows(rows))


def run_summary_footer(report: SyncReport) -> str:
    """One-line summary: table total, changed/unchanged/failed counts, duration."""
    counts = report.counts
    return (
        f"{counts.total} tables: {counts.changed} changed, {counts.unchanged} unchanged, "
        f"{counts.failed} failed ({report.duration_seconds:.1f}s)"
    )


def _heading(text: str, rule: str = "=") -> str:
    """Render a section title underlined with a rule the width of the text."""
    return f"{text}\n{rule * len(text)}"


def _dry_run_banner(report: SyncReport) -> str:
    """Return the dry-run banner, or empty for an applied run."""
    return "PLAN — no planned SQL executed" if report.dry_run else ""


def render_failures_section(reports: Sequence[TableRunReport]) -> str:
    """Render full per-table failure detail for every failed table; empty when none failed."""
    failed = [report for report in reports if report.has_failures]
    if not failed:
        return ""
    blocks: list[str] = []
    for report in failed:
        lines = [f"  {report.qualified_name}"]
        for failure in report.failures:
            # A failure leads with what went wrong; anything after it is
            # supporting detail, nested a level deeper.
            head, *supporting = failure.format_lines()
            lines.append(f"    {head}")
            lines.extend(f"        {line}" for line in supporting)
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
