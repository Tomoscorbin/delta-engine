"""CLI-private rendering for one complete schema plan."""

from delta_engine.application import SyncReport, render_diff, render_report
from delta_engine.cli.connection import Target
from delta_engine.cli.declarations import DeclarationRef


def render_plan(
    target: Target,
    declaration: DeclarationRef,
    report: SyncReport,
) -> str:
    """Render identity, semantic diff, report, and any planned SQL in order."""
    sections = [
        _render_identity(target, declaration),
        render_diff(report),
        render_report(report),
    ]
    planned_sql = _render_planned_sql(report)
    if planned_sql:
        sections.append(planned_sql)
    return "\n\n".join(sections)


def _render_identity(target: Target, declaration: DeclarationRef) -> str:
    """Render only non-credential target values plus the declaration reference."""
    return "\n".join(
        (
            _heading("TARGET"),
            f"Host: {target.host}",
            f"SQL warehouse: {target.warehouse_id}",
            f"Declarations: {declaration}",
        )
    )


def _render_planned_sql(report: SyncReport) -> str:
    """Render exact planned statements without exposing a library API."""
    planned = report.planned_sql_statements
    if not planned:
        return ""
    blocks = ["\n".join([f"-- {name}", *statements]) for name, statements in planned.items()]
    return "\n\n".join([_heading("PLANNED SQL"), *blocks])


def _heading(text: str) -> str:
    """Render a CLI section heading."""
    return f"{text}\n{'=' * len(text)}"
