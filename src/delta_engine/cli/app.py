"""
The delta-engine command-line application.

Commands are thin orchestrations: load explicit declarations, open a unified-
authentication SQL connection, run ``Engine.sync`` through the warehouse
backend, render the report, and map it to an exit code.
"""

from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
from enum import StrEnum
import json
import logging
import sys
from typing import Annotated

import typer

import delta_engine
from delta_engine.application import (
    SyncFailedError,
    SyncReport,
    render_diff,
    render_planned_sql,
    render_report,
)
from delta_engine.cli.connection import open_connection
from delta_engine.cli.declarations import load_declarations
from delta_engine.cli.errors import ConfigError
from delta_engine.databricks import build_sql_engine

app = typer.Typer(
    name="delta-engine",
    help="Declarative schema management for Delta Lake tables on Databricks.",
    no_args_is_help=True,
    # Plain tracebacks, matching reference-cli.md and keeping locals (which
    # can hold connection settings) out of CI logs.
    pretty_exceptions_enable=False,
)


class OutputFormat(StrEnum):
    """Report formats the CLI can emit."""

    TEXT = "text"
    JSON = "json"


_EXIT_SUCCESS = 0
_EXIT_FAILURES = 1
_EXIT_CHANGES_PENDING = 2

SpecsArgument = Annotated[
    list[str],
    typer.Argument(
        metavar="MODULE:ATTRIBUTE...",
        help=(
            "Explicit declaration attributes, e.g. "
            "myproject.tables:orders or myproject.tables:all_tables."
        ),
    ),
]
OutputOption = Annotated[OutputFormat, typer.Option("--output", help="Report format on stdout.")]
ShowSqlOption = Annotated[
    bool,
    typer.Option(
        "--show-sql",
        help="Append each table's planned SQL (text output; JSON always carries it).",
    ),
]
HostOption = Annotated[
    str | None,
    typer.Option("--host", help="Overrides DATABRICKS_HOST."),
]
HttpPathOption = Annotated[
    str | None, typer.Option("--http-path", help="Overrides DATABRICKS_HTTP_PATH.")
]
ProfileOption = Annotated[
    str | None,
    typer.Option("--profile", help="Overrides DATABRICKS_CONFIG_PROFILE."),
]
VerboseOption = Annotated[
    bool, typer.Option("--verbose", "-v", help="Show engine progress (INFO) on stderr.")
]
FailOnChangesOption = Annotated[
    bool,
    typer.Option(
        "--fail-on-changes",
        help="Exit 2 when a valid plan contains pending changes.",
    ),
]


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(delta_engine.__version__)
        raise typer.Exit()


@app.callback()
def _main(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_version_callback, is_eager=True),
    ] = False,
) -> None:
    """Handle the global --version option."""


@app.command()
def plan(
    specs: SpecsArgument,
    output: OutputOption = OutputFormat.TEXT,
    show_sql: ShowSqlOption = False,
    host: HostOption = None,
    http_path: HttpPathOption = None,
    profile: ProfileOption = None,
    verbose: VerboseOption = False,
    fail_on_changes: FailOnChangesOption = False,
) -> None:
    """Dry-run declarations; valid plans succeed unless --fail-on-changes is set."""
    with _anticipated_errors():
        report = _sync(
            specs,
            host,
            http_path,
            profile,
            verbose,
            output,
            dry_run=True,
        )
        _emit(report, output, show_sql, include_diff=True)
        raise typer.Exit(code=_plan_exit_code(report, fail_on_changes=fail_on_changes))


@app.command()
def apply(
    specs: SpecsArgument,
    output: OutputOption = OutputFormat.TEXT,
    show_sql: ShowSqlOption = False,
    host: HostOption = None,
    http_path: HttpPathOption = None,
    profile: ProfileOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Sync declarations to the catalog; exit 1 when any table fails."""
    with _anticipated_errors():
        try:
            report = _sync(
                specs,
                host,
                http_path,
                profile,
                verbose,
                output,
                dry_run=False,
            )
        except SyncFailedError as error:
            _emit(error.report, output, show_sql, include_diff=False)
            raise typer.Exit(code=_EXIT_FAILURES) from None
        _emit(report, output, show_sql, include_diff=False)
        raise typer.Exit(code=_EXIT_SUCCESS)


def _plan_exit_code(report: SyncReport, *, fail_on_changes: bool) -> int:
    if report.has_failures:
        return _EXIT_FAILURES
    if fail_on_changes and report.has_changes:
        return _EXIT_CHANGES_PENDING
    return _EXIT_SUCCESS


@contextmanager
def _engine_logging(verbose: bool) -> Iterator[None]:
    """
    Attach a per-invocation stderr handler to the package logger.

    Scoped and removed afterwards so repeated in-process invocations (tests,
    embedders) never leave a handler bound to a closed stream, and the CLI
    never takes over an embedding application's root logger.
    """
    package_logger = logging.getLogger("delta_engine")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s | %(name)s | %(message)s"))
    previous_level = package_logger.level
    package_logger.addHandler(handler)
    package_logger.setLevel(logging.INFO if verbose else logging.WARNING)
    try:
        yield
    finally:
        package_logger.removeHandler(handler)
        package_logger.setLevel(previous_level)


def _sync(
    spec_texts: list[str],
    host: str | None,
    http_path: str | None,
    profile: str | None,
    verbose: bool,
    output: OutputFormat,
    *,
    dry_run: bool,
) -> SyncReport:
    """Load declarations, open one connection, and run one sync."""
    with _engine_logging(verbose):
        if output is OutputFormat.JSON:
            # User declarations, authentication providers, and connector code
            # can print. Keep machine-readable stdout reserved for the final
            # payload.
            with redirect_stdout(sys.stderr):
                return _run_sync(
                    spec_texts,
                    host,
                    http_path,
                    profile,
                    dry_run=dry_run,
                )
        return _run_sync(spec_texts, host, http_path, profile, dry_run=dry_run)


def _run_sync(
    spec_texts: list[str],
    host: str | None,
    http_path: str | None,
    profile: str | None,
    *,
    dry_run: bool,
) -> SyncReport:
    tables = load_declarations(spec_texts)
    with open_connection(host, http_path, profile) as connection:
        engine = build_sql_engine(connection)
        return engine.sync(*tables, dry_run=dry_run)


def _emit(report: SyncReport, output: OutputFormat, show_sql: bool, *, include_diff: bool) -> None:
    if output is OutputFormat.JSON:
        typer.echo(json.dumps(report.to_dict(), indent=2))
        return
    sections = []
    if include_diff:
        sections.append(render_diff(report))
    sections.append(render_report(report))
    if show_sql:
        planned = render_planned_sql(report)
        if planned:
            sections.append(planned)
    typer.echo("\n\n".join(sections))


@contextmanager
def _anticipated_errors() -> Iterator[None]:
    """Render anticipated configuration failures; let user-code defects propagate."""
    try:
        yield
    except ConfigError as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(code=_EXIT_FAILURES) from None
