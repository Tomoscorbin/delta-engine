"""CLI-private rendering for one complete sync run."""

from delta_engine.application import SyncReport, render_diff, render_report
from delta_engine.cli.connection import Target
from delta_engine.cli.declarations import DeclarationRef


def render_sync(
    target: Target,
    declaration: DeclarationRef,
    report: SyncReport,
) -> str:
    """Render identity, semantic diff, report, and any compiled SQL in order."""
    sections = [
        _render_identity(target, declaration),
        render_diff(report),
        render_report(report),
    ]
    sql = _render_sql(report)
    if sql:
        sections.append(sql)
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


def _render_sql(report: SyncReport) -> str:
    """Render exact compiled statements under a heading that states their fate."""
    planned = report.planned_sql_statements
    if not planned:
        return ""
    heading = "PLANNED SQL" if report.dry_run else "EXECUTED SQL"
    blocks = ["\n".join([f"-- {name}", *statements]) for name, statements in planned.items()]
    return "\n\n".join([_heading(heading), *blocks])


def _heading(text: str) -> str:
    """Render a CLI section heading."""
    return f"{text}\n{'=' * len(text)}"
