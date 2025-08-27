from __future__ import annotations
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence

# Expect these types from your codebase:
# - RunReport { run_id, started_at, ended_at, tables: dict[str, TableExecutionReport] }
# - TableExecutionReport { table_name, results: tuple[ActionResult, ...], started_at, ended_at, status }
# - ActionResult { action_index, status, statement_preview, failure? }
# - ActionStatus Enum: OK, FAILED, SKIPPED_DEPENDENCY, NOOP
# - ValidationRunReport { run_id, started_at, ended_at, tables: dict[str, TableValidationReport] }
# - TableValidationReport { table_name, failures: tuple[ValidationFailure, ...], started_at, ended_at, status }
# - ValidationFailure { rule_name, table_name, message }

# ---------- Generic helpers ----------

def parse_iso8601(dt: str) -> datetime:
    try:
        return datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)

def human_duration(start_iso: str, end_iso: str) -> str:
    start = parse_iso8601(start_iso)
    end = parse_iso8601(end_iso)
    seconds = max(0, int((end - start).total_seconds()))
    if seconds < 1:
        return "<1s"
    mins, secs = divmod(seconds, 60)
    hours, mins = divmod(mins, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if mins:
        parts.append(f"{mins}m")
    parts.append(f"{secs}s")
    return " ".join(parts)

def ellipsize(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    if max_chars <= 1:
        return "…"
    return text[: max_chars - 1] + "…"

def plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"

# ---------- Execution report rendering ----------

def format_execution_summary(executions: Mapping[str, "TableExecutionReport"]) -> str:
    statuses = [t.status for t in executions.values()]
    counts = Counter(statuses)
    total = len(statuses)
    ok = counts.get("OK", 0)
    failed = counts.get("FAILED", 0)
    skipped = counts.get("SKIPPED_DEPENDENCY", 0)
    noop = counts.get("NOOP", 0)
    return (
        f"{plural(total, 'table')} planned — "
        f"{ok} OK, {failed} FAILED, {skipped} SKIPPED, {noop} NOOP"
    )

def _action_counts(t: "TableExecutionReport") -> tuple[int, int, int, int]:
    ok = sum(1 for r in t.results if r.status == "OK")
    failed = sum(1 for r in t.results if r.status == "FAILED")
    skipped = sum(1 for r in t.results if r.status == "SKIPPED_DEPENDENCY")
    noop = sum(1 for r in t.results if r.status == "NOOP")
    return ok, failed, skipped, noop

def format_execution_table(executions: Mapping[str, "TableExecutionReport"], max_width: int = 100) -> str:
    # Column widths (simple, deterministic)
    status_w = 8
    duration_w = 8
    actions_w = 13  # "ok/failed/skip"
    table_w = max(20, min(40, max((len(name) for name in executions), default=20)))
    first_issue_w = max_width - (status_w + 1 + table_w + 1 + actions_w + 1 + duration_w) - 5
    first_issue_w = max(20, first_issue_w)

    header = f"{'STATUS':<{status_w}} {'TABLE':<{table_w}} {'ACTIONS':<{actions_w}} {'DURATION':<{duration_w}} ISSUE"
    sep = "-" * len(header)

    lines = [header, sep]
    # Stable order: failures first, then alphabetical
    def sort_key(item):
        name, rep = item
        return (0 if rep.status == "FAILED" else 1, name)

    for name, rep in sorted(executions.items(), key=sort_key):
        ok, failed, skipped, noop = _action_counts(rep)
        actions_cell = f"{ok}/{failed}/{skipped}"  # omit NOOP from the cell; it’s noise
        duration_cell = human_duration(rep.started_at, rep.ended_at)
        issue_preview = ""
        if rep.status == "FAILED":
            # First failure message preview
            failures = [r for r in rep.results if getattr(r, "failure", None)]
            if failures:
                f0 = failures[0].failure  # type: ignore[assignment]
                issue_preview = ellipsize(f0.message.replace("\n", " "), first_issue_w)
        line = (
            f"{rep.status:<{status_w}} "
            f"{ellipsize(name, table_w):<{table_w}} "
            f"{actions_cell:<{actions_w}} "
            f"{duration_cell:<{duration_w}} "
            f"{issue_preview}"
        )
        lines.append(line)
    return "\n".join(lines)

def format_execution_failures(executions: Mapping[str, "TableExecutionReport"]) -> str:
    # Detailed blocks for failed tables
    blocks: list[str] = []
    for name, rep in executions.items():
        if rep.status != "FAILED":
            continue
        lines = [name]
        for r in rep.results:
            if not getattr(r, "failure", None):
                continue
            f = r.failure  # type: ignore[assignment]
            lines += [
                f"- action #{r.action_index} | {f.exception_type}",
                f"  {f.message}",
                f"  SQL: {ellipsize(r.statement_preview, 180)}",
            ]
        blocks.append("\n".join(lines))
    return "\n----\n\n".join(blocks) if blocks else ""

def format_run_report(run_report: "RunReport", max_width: int = 100) -> str:
    header = (
        f"RUN {run_report.run_id} | "
        f"{run_report.started_at} → {run_report.ended_at} "
        f"({human_duration(run_report.started_at, run_report.ended_at)})"
    )
    summary = format_execution_summary(run_report.executions)
    table = format_execution_table(run_report.executions, max_width=max_width)
    failures = format_execution_failures(run_report.executions)
    parts = [header, summary, "", table]
    if failures:
        parts += ["", "Failures:", failures]
    return "\n".join(parts)

# ---------- Validation report rendering ----------

def format_validation_summary(tables: Mapping[str, "TableValidationReport"]) -> str:
    statuses = [t.status for t in tables.values()]
    counts = Counter(statuses)
    total = len(statuses)
    passed = counts.get("PASSED", 0)
    failed = counts.get("FAILED", 0)
    return f"{plural(total, 'table')} validated — {passed} PASSED, {failed} FAILED"

def format_validation_table(tables: Mapping[str, "TableValidationReport"], max_width: int = 100) -> str:
    status_w = 8
    table_w = max(20, min(40, max((len(name) for name in tables), default=20)))
    duration_w = 8
    rule_w = max_width - (status_w + 1 + table_w + 1 + duration_w) - 5
    rule_w = max(20, rule_w)

    header = f"{'STATUS':<{status_w}} {'TABLE':<{table_w}} {'DURATION':<{duration_w}} RULE / MESSAGE"
    sep = "-" * len(header)
    lines = [header, sep]

    def sort_key(item):
        name, rep = item
        return (0 if rep.status == "FAILED" else 1, name)

    for name, rep in sorted(tables.items(), key=sort_key):
        duration_cell = human_duration(rep.started_at, rep.ended_at)
        rule_message = ""
        if rep.failures:
            f0 = rep.failures[0]
            rule_message = ellipsize(f"{f0.rule_name}: {f0.message.replace('\n', ' ')}", rule_w)
        lines.append(
            f"{rep.status:<{status_w}} {ellipsize(name, table_w):<{table_w}} {duration_cell:<{duration_w}} {rule_message}"
        )
    return "\n".join(lines)

def format_validation_failures(tables: Mapping[str, "TableValidationReport"]) -> str:
    blocks: list[str] = []
    for name, rep in tables.items():
        if rep.failures:
            lines = [name] + [f"- {f.rule_name}\n  {f.message}" for f in rep.failures]
            blocks.append("\n".join(lines))
    return "\n----\n\n".join(blocks) if blocks else ""

def format_validation_run_report(validation_report: "ValidationRunReport", max_width: int = 100) -> str:
    header = (
        f"VALIDATION {validation_report.run_id} | "
        f"{validation_report.started_at} → {validation_report.ended_at} "
        f"({human_duration(validation_report.started_at, validation_report.ended_at)})"
    )
    summary = format_validation_summary(validation_report.validations)
    table = format_validation_table(validation_report.validations, max_width=max_width)
    failures = format_validation_failures(validation_report.validations)
    parts = [header, summary, "", table]
    if failures:
        parts += ["", "Failures:", failures]
    return "\n".join(parts)
