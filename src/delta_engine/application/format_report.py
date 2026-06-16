"""
Human-readable formatting for sync failures.

Turns a failed `TableRunReport` into the per-table detail lines shared by the
`SyncFailedError` message.
"""

from __future__ import annotations

from delta_engine.application.results import TableRunReport


def format_failure_detail(table_report: TableRunReport) -> list[str]:
    """
    Return the detail lines describing why a single table failed.

    Covers the table headline, each top-level failure (read, validation,
    execution), and SQL previews for any failed actions.
    """
    lines = [f"\n❌ {table_report.fully_qualified_name} [{table_report.status.value}]"]

    for failure in table_report.all_failures:
        lines.append(f"    {failure.format_line()}")

    for result in table_report.execution_results:
        if result.failure:
            lines.append(f"    Failed SQL preview (action {result.action_index}):")
            lines.append(f"        {result.statement_preview}")

    return lines
