"""Human-readable formatting for sync run reports.

Transforms a `SyncReport` into a concise text summary: normalizes timestamps,
computes durations, summarizes per-table status, and includes brief previews of
failed actions when present.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from delta_engine.application.results import ActionStatus, SyncReport, TableRunStatus


def _as_dt(v: str | datetime) -> datetime:
    return v if isinstance(v, datetime) else datetime.fromisoformat(v)


def _fmt_td(td: timedelta) -> str:
    return f"{td.total_seconds():.2f}s"


def format_sync_report(report: SyncReport) -> str:
    """Return a human-readable summary of a SyncReport (new results model)."""
    started_dt = _as_dt(report.started_at)
    ended_dt = _as_dt(report.ended_at)
    duration_td = ended_dt - started_dt
    duration_str = _fmt_td(duration_td)

    lines: list[str] = []
    lines.append(f"Sync run: {started_dt.isoformat()} → {ended_dt.isoformat()} ({duration_str})")

    # --- Header
    lines.append(f"{started_dt:%Y-%m-%d %H:%M:%S} | INFO | Engine Sync Report")
    lines.append("=" * 55)
    lines.append(f"Started:  {started_dt.isoformat()}")
    lines.append(f"Ended:    {ended_dt.isoformat()}")
    lines.append(f"Duration: {int(duration_td.total_seconds())}s")
    lines.append("")

    total = len(report.table_reports)
    failed = sum(1 for t in report.table_reports if t.has_failures)
    ok = total - failed
    lines.append(f"Tables: {total}  |  OK: {ok}  |  Failed: {failed}")
    lines.append("─" * 78)

    # --- Per-table summaries
    for t in report.table_reports:
        status_icon = "❌" if t.has_failures else "✅"
        status_text = t.status.value

        # duration per table
        ts = _as_dt(t.started_at)
        te = _as_dt(t.ended_at)
        t_dur = _fmt_td(te - ts)

        # summary by status (no use of removed t.failure)
        if t.status is TableRunStatus.READ_FAILED:
            summary = "read failed"
        elif t.status is TableRunStatus.VALIDATION_FAILED:
            n = len(t.validation.failures) if (t.validation and t.validation.failed) else 1
            summary = f"{n} validation failure(s)"
        elif t.status is TableRunStatus.EXECUTION_FAILED:
            total_actions = len(t.execution_results or ())
            failed_actions = sum(
                1 for r in (t.execution_results or ()) if r.status is ActionStatus.FAILED
            )
            summary = f"{failed_actions}/{total_actions} actions failed"
        else:
            total_actions = len(t.execution_results or ())
            summary = f"executed {total_actions} actions" if total_actions else "no-op"

        lines.append(
            f"{status_icon} {t.fully_qualified_name:<30} {summary:<28} {status_text} ({t_dur})"
        )

    # --- Footer
    lines.append("─" * 78)
    lines.append("Run completed with failures." if failed else "All tables synced successfully ✅")

    return "\n".join(lines)
