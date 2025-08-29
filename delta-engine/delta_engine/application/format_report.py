from datetime import datetime
from delta_engine.application.results import SyncReport

def format_sync_report(report: SyncReport) -> str:
    """Return a human-readable summary of a SyncReport, like dbt output."""
    lines: list[str] = []

    started = datetime.fromisoformat(report.started_at)
    ended = datetime.fromisoformat(report.ended_at)
    duration = ended - started

    # --- Header
    lines.append(f"{started:%Y-%m-%d %H:%M:%S} | INFO | Engine Sync Report")
    lines.append("=" * 55)
    lines.append(f"Started:  {report.started_at}")
    lines.append(f"Ended:    {report.ended_at}")
    lines.append(f"Duration: {duration.total_seconds():.0f}s")
    lines.append("")

    total = len(report.table_reports)
    failed = sum(1 for t in report.table_reports if t.has_failures())
    ok = total - failed
    lines.append(f"Tables: {total}  |  OK: {ok}  |  Failed: {failed}")
    lines.append("─" * 78)

    # --- Per-table summaries
    for tbl in report.table_reports:
        status_icon = "❌" if tbl.has_failures() else "✅"
        status_text = tbl.status.value

        # main summary line
        if tbl.execution_results:
            action_count = len(tbl.execution_results)
            summary = f"(executed {action_count} actions)"
        elif tbl.failure:
            summary = f"({len(tbl.failure)} failures)"
        else:
            summary = "(no-op)"

        lines.append(
            f"{status_icon} {tbl.fully_qualified_name:<30} {summary:<25} {status_text}"
        )

        # show detailed failures, if any
        if tbl.failure:
            for fail in tbl.failure:
                lines.append(f"    - {fail.rule_name if hasattr(fail,'rule_name') else fail.exception_type}: {fail.message}")

    # --- Footer
    lines.append("─" * 78)
    if failed:
        lines.append("Run completed with failures. See details above.")
    else:
        lines.append("All tables synced successfully ✅")

    return "\n".join(lines)
