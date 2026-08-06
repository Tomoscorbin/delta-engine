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
from delta_engine.application.report import SyncReport, TableChangeState, TableRun
from delta_engine.domain.plan import ActionPlan, TableDrift

# Shown wherever a report has a readable state but no planned actions. One
# spelling, two presentations: bare in the grid's DETAIL cell, parenthesised as
# a standalone line in the diff block.
_NO_CHANGES: Final[str] = "no changes"

_DETAIL_MAX_CHARS: Final[int] = 60

_GRID_HEADERS: Final[tuple[str, str, str, str]] = ("TABLE", "STATUS", "STATEMENTS", "DETAIL")

_REAL_RUN_OUTCOME_ORDER: Final[tuple[TableChangeState, ...]] = (
    TableChangeState.APPLIED,
    TableChangeState.PARTIALLY_APPLIED,
    TableChangeState.NOT_APPLIED,
    TableChangeState.UNCHANGED,
    TableChangeState.NOT_PLANNED,
)


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


def _change_outcome_marker(report: TableRun, change_state: TableChangeState | None) -> str | None:
    """Describe an incomplete real-run outcome; omit previews and complete outcomes."""
    if change_state is TableChangeState.NOT_APPLIED:
        return change_state.value
    if change_state is TableChangeState.PARTIALLY_APPLIED:
        progress = report.statement_progress
        if progress is None:
            raise ValueError("A partially applied change requires execution progress")
        return f"{change_state.value}, {progress.applied}/{progress.planned}"
    return None


def _render_diff_block(report: TableRun, *, change_state: TableChangeState | None) -> str:
    """Render one table's change block, optionally marking its aggregate outcome."""
    header = str(report.qualified_name)
    if isinstance(report.read, ReadFailure):
        return f"{header}\n  (could not read — no diff)"

    plan = report.plan
    if plan is None:
        # No plan means the diff was rejected, which is not the same as having
        # found nothing. The failures section says which rule rejected; this
        # says what it rejected.
        rejected = drift_entries(report.diff) if isinstance(report.diff, TableDrift) else ()
        if rejected:
            return "\n".join(
                [f"{header}  (REJECTED — no SQL planned)", *_render_entry_groups(rejected)]
            )
        return f"{header}\n  ({_NO_CHANGES} — see failures)"

    if not plan:
        if report.has_failures:
            return f"{header}\n  ({_NO_CHANGES} — see failures)"
        return f"{header}\n  ({_NO_CHANGES})"

    notes = ["CREATE"] if report.creates_table else []
    if marker := _change_outcome_marker(report, change_state):
        notes.append(marker)
    if notes:
        header = f"{header}  ({' — '.join(notes)})"
    return "\n".join([header, *_render_entry_groups(plan_entries(plan))])


def render_diff_block(report: TableRun) -> str:
    """Render one table's planned change block without claiming a run outcome."""
    return _render_diff_block(report, change_state=None)


def _grid_statements_cell(report: TableRun, change_state: TableChangeState | None = None) -> str:
    """STATEMENTS cell: execution progress, compiled count, or — when none compiled."""
    progress = report.statement_progress
    if progress is not None:
        return f"{progress.applied}/{progress.planned}"
    planned = len(report.compiled.statements) if report.compiled is not None else 0
    if report.has_failures:
        return f"0/{planned}" if change_state is TableChangeState.NOT_APPLIED and planned else "—"
    return str(planned)


def _humanized_action_summary(plan: ActionPlan | None) -> str:
    """Summarise a plan as per-category change counts in display order, e.g. '2 columns, 1 key'."""
    if plan is None:
        return _NO_CHANGES
    counts = Counter(entry.category for entry in plan_entries(plan))
    parts = [category.counted(counts[category]) for category in DiffCategory if counts[category]]
    return ", ".join(parts) or _NO_CHANGES


def _grid_detail(report: TableRun) -> str:
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


def _grid_row_cells(
    report: TableRun, change_state: TableChangeState | None = None
) -> tuple[str, str, str, str]:
    """Return the four grid cells for one report (DETAIL already truncated)."""
    return (
        str(report.qualified_name),
        report.status.value,
        _grid_statements_cell(report, change_state),
        _truncate(_grid_detail(report)),
    )


def render_grid(reports: Sequence[TableRun]) -> str:
    """Render an aligned TABLE | STATUS | STATEMENTS | DETAIL grid for ``reports``."""
    rows = [_GRID_HEADERS, *(_grid_row_cells(report) for report in reports)]
    return "\n".join(_aligned_rows(rows))


def _render_run_grid(report: SyncReport) -> str:
    """Render the grid with catalog outcomes supplied by the validated aggregate."""
    rows = [
        _GRID_HEADERS,
        *(
            _grid_row_cells(table_report, change_state)
            for table_report, change_state in zip(
                report.table_runs, report.table_change_states, strict=True
            )
        ),
    ]
    return "\n".join(_aligned_rows(rows))


def _count_phrase(count: int, singular: str, plural: str) -> str:
    """Render a count with the noun that agrees with it."""
    return f"{count} {singular if count == 1 else plural}"


def run_summary_footer(report: SyncReport) -> str:
    """Summarise planned work for a preview or catalog outcomes for a real run."""
    total = _count_phrase(len(report.table_runs), "table", "tables")
    summary = _planned_summary(report) if report.dry_run else _real_run_summary(report)
    prefix = f"{total}: {summary}" if summary else total
    return f"{prefix} ({report.duration_seconds:.1f}s)"


def _planned_summary(report: SyncReport) -> str:
    """Count changed, unchanged, and failed tables without claiming an outcome."""
    counts = report.counts
    return f"{counts.changed} changed, {counts.unchanged} unchanged, {counts.failed} failed"


def _real_run_summary(report: SyncReport) -> str:
    """Count each table by the catalog change state derived by the aggregate."""
    counts = Counter(report.table_change_states)
    return ", ".join(
        f"{counts[state]} {state.value}" for state in _REAL_RUN_OUTCOME_ORDER if counts[state]
    )


def _heading(text: str, rule: str = "=") -> str:
    """Render a section title underlined with a rule the width of the text."""
    return f"{text}\n{rule * len(text)}"


def _dry_run_banner(report: SyncReport) -> str:
    """Return the dry-run banner, or empty for an applied run."""
    return "PLAN — no planned SQL executed" if report.dry_run else ""


def render_failures_section(reports: Sequence[TableRun]) -> str:
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
        _render_run_grid(report),
        render_failures_section(report.table_runs),
        run_summary_footer(report),
    )
    return "\n\n".join(part for part in parts if part)


def render_diff(report: SyncReport) -> str:
    """Render every table's planned changes as +/-/~ blocks, under a DIFF title."""
    blocks = [
        _render_diff_block(table_report, change_state=change_state)
        for table_report, change_state in zip(
            report.table_runs, report.table_change_states, strict=True
        )
    ]
    return "\n\n".join([_heading("DIFF"), *blocks])
