from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping


def format_run_report(
    report: "RunReport",
    *,
    show_only_failures: bool = False,
    max_error_chars: int = 800,
    max_failures_per_table: int | None = None,
    use_color: bool = True,
) -> str:
    """Return a readable, single-string run report.

    Shows all tables (or only failures), marks successes with ✅ and failures with ❌,
    lists operations for successes, and summarizes failure messages for failures.
    """
    lines: list[str] = []

    started = _parse_iso(report.started_at)
    ended = _parse_iso(report.ended_at)
    duration = _human_duration(started, ended) if (started and ended) else "n/a"

    executions = dict(sorted(report.executions.items(), key=lambda kv: kv[0]))
    total_tables = len(executions)

    success_tables: list[str] = []
    failed_tables: list[str] = []

    # First pass: classify
    for table_name, exec_report in executions.items():
        if _is_failed(exec_report):
            failed_tables.append(table_name)
        else:
            success_tables.append(table_name)

    # Header / summary
    lines.append(_header(f"EXECUTION RUN {report.run_id}"))
    lines.append(
        f"Started: {report.started_at}  |  Ended: {report.ended_at}  |  Duration: {duration}"
    )
    lines.append(
        f"Tables: {total_tables}  |  OK: {len(success_tables)}  |  Failed: {len(failed_tables)}"
    )
    lines.append(_rule())

    # Successes
    if not show_only_failures and success_tables:
        lines.append("Succeeded tables:")
        for table_name in success_tables:
            exec_report = executions[table_name]
            icon = _green('✅') if use_color else '✅'
            status = _status_name(exec_report)
            seq = _action_sequence(exec_report.results) or "(no-op)"
            lines.append(f"  {icon} {table_name}    {seq}  ({status})")
        lines.append(_rule())

    # Failures
    if failed_tables:
        lines.append("Failed tables:")
        for table_name in failed_tables:
            exec_report = executions[table_name]
            icon = _red('❌') if use_color else '❌'
            status = _status_name(exec_report)
            seq = _action_sequence(exec_report.results)
            lines.append(f"  {icon} {table_name}    {seq}  ({status})")
            lines.append(f"  {icon} {table_name}  {action_counts}  ({status})")
            lines.extend(_format_execution_failures(exec_report, max_error_chars, max_failures_per_table))
        lines.append(_rule())

    return "\n".join(lines)


# -------------------------- helpers --------------------------

def _is_failed(exec_report: Any) -> bool:
    """Heuristic: FAILED status or any result with a failure attribute."""
    status = getattr(exec_report, "status", None)
    # Enum-like with .name or plain string
    status_name = status.name if hasattr(status, "name") else (status or "")
    status_name = str(status_name).upper()
    if status_name == "FAILED":
        return True

    results: Iterable[Any] = getattr(exec_report, "results", ()) or ()
    for result in results:
        if getattr(result, "failure", None):
            return True
    return False


def _status_name(exec_report: Any) -> str:
    status = getattr(exec_report, "status", None)
    if hasattr(status, "name"):
        return status.name
    if status:
        return str(status)
    return "UNKNOWN"


def _format_action_counts(counts: Mapping[str, int] | None) -> str:
    if not counts:
        return ""
    ordered = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    return f"[ {ordered} ]"


def _format_execution_failures(
    exec_report: Any,
    max_error_chars: int,
    max_failures_per_table: int | None,
) -> list[str]:
    lines: list[str] = []
    results: Iterable[Any] = getattr(exec_report, "results", ()) or ()
    failures = []
    for res in results:
        failure = getattr(res, "failure", None)
        if failure:
            failures.append(failure)

    # Fallback: some executors may store messages at top level
    if not failures:
        messages = getattr(exec_report, "messages", ()) or ()
        for i, msg in enumerate(messages, start=1):
            short = str(msg)
            lines.append(f"      {i:>2}. {short[:max_error_chars]}")
        return lines

    # Limit count if requested
    display_failures = failures if max_failures_per_table is None else failures[:max_failures_per_table]

    for i, failure in enumerate(display_failures, start=1):
        kind = getattr(failure, "kind", None) or getattr(failure, "type", None) or type(failure).__name__
        summary = (
            getattr(failure, "summary", None)
            or getattr(failure, "message", None)
            or str(failure)
        )
        detail = getattr(failure, "details", None) or getattr(failure, "traceback", None) or ""
        lines.append(f"      {i:>2}. {kind}: {summary}")
        if detail:
            padded = "         " + _truncate(detail, max_error_chars).replace("\n", "\n         ")
            lines.append(padded)

    hidden = len(failures) - len(display_failures)
    if hidden > 0:
        lines.append(f"         … {hidden} more not shown")
    return lines


def _action_sequence(results: Iterable["ActionResult"]) -> str:
    names = [r.name for r in results if getattr(r, "name", None)]
    if not names:
        return ""
    # Collapse consecutive duplicates for compactness
    collapsed: list[tuple[str, int]] = []
    for n in names:
        if collapsed and collapsed[-1][0] == n:
            last_name, last_cnt = collapsed[-1]
            collapsed[-1] = (last_name, last_cnt + 1)
        else:
            collapsed.append((n, 1))
    parts = [f"{n}×{c}" if c > 1 else n for n, c in collapsed]
    return ", ".join(parts)


def _header(text: str) -> str:
    return f"{text}\n{'=' * len(text)}"


def _rule(char: str = "─", width: int = 80) -> str:
    return char * width


def _truncate(text: str, max_chars: int) -> str:
    return text if len(text) <= max_chars else text[: max(0, max_chars - 1)] + "…"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _human_duration(start: datetime | None, end: datetime | None) -> str:
    if not start or not end:
        return "n/a"
    delta = end - start
    total_ms = int(delta.total_seconds() * 1000)
    ms = total_ms % 1000
    s = (total_ms // 1000) % 60
    m = (total_ms // 60_000) % 60
    h = total_ms // 3_600_000
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    if s:
        return f"{s}s {ms}ms"
    return f"{ms}ms"


# Optional ANSI coloring (kept tiny and local)

def _green(text: str) -> str:
    return f"\x1b[32m{text}\x1b[0m"

def _red(text: str) -> str:
    return f"\x1b[31m{text}\x1b[0m"
