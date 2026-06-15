"""
Human-readable formatting for sync run reports.

Transforms a `SyncReport` into a concise text summary: computes durations,
summarizes per-table status, and includes brief previews of failed actions
when present.
"""

from __future__ import annotations

from datetime import timedelta

from delta_engine.application.results import ActionStatus, SyncReport, TableRunStatus


def _format_duration(duration: timedelta) -> str:
    """Format a timedelta as seconds with two decimals (e.g., '1.23s')."""
    return f"{duration.total_seconds():.2f}s"


def format_sync_report(report: SyncReport) -> str:
    """Return a human-readable summary of a SyncReport."""
    started = report.started_at
    ended = report.ended_at
    duration = ended - started
    duration_str = _format_duration(duration)

    lines: list[str] = []
    lines.append(f"Sync run: {started.isoformat()} → {ended.isoformat()} ({duration_str})")

    # --- Header
    lines.append(f"{started:%Y-%m-%d %H:%M:%S} | INFO | Engine Sync Report")
    lines.append("=" * 55)
    lines.append(f"Started:  {started.isoformat()}")
    lines.append(f"Ended:    {ended.isoformat()}")
    lines.append(f"Duration: {int(duration.total_seconds())}s")
    lines.append("")

    total = len(report.table_reports)
    failed = sum(1 for table in report.table_reports if table.has_failures)
    ok = total - failed
    lines.append(f"Tables: {total}  |  OK: {ok}  |  Failed: {failed}")
    lines.append("─" * 78)

    # --- Per-table summaries
    for table in report.table_reports:
        status_icon = "❌" if table.has_failures else "✅"
        status_text = table.status.value
        table_duration = _format_duration(table.ended_at - table.started_at)

        if table.status is TableRunStatus.READ_FAILED:
            summary = "read failed"
        elif table.status is TableRunStatus.VALIDATION_FAILED:
            summary = f"{len(table.validation.failures)} validation failure(s)"
        elif table.status is TableRunStatus.EXECUTION_FAILED:
            total_actions = len(table.execution_results)
            failed_actions = sum(
                1 for result in table.execution_results if result.status is ActionStatus.FAILED
            )
            summary = f"{failed_actions}/{total_actions} actions failed"
        else:
            total_actions = len(table.execution_results)
            summary = f"executed {total_actions} actions" if total_actions else "no-op"

        lines.append(
            f"{status_icon} {table.fully_qualified_name:<30} "
            f"{summary:<28} {status_text} ({table_duration})"
        )

    # --- Footer
    lines.append("─" * 78)
    lines.append("Run completed with failures." if failed else "All tables synced successfully ✅")

    return "\n".join(lines)
