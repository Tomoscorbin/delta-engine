"""
The delta-engine command-line application.

Commands are thin orchestrations: resolve settings, load declarations, run
``Engine.sync`` through the warehouse backend, render the report, and map it
to an exit code. Every decision lives in ``declarations.py``,
``connection.py``, or the engine itself.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from enum import StrEnum
import json
import logging
import os
import sys
import traceback
from typing import TYPE_CHECKING, Annotated

import typer

import delta_engine
from delta_engine.application import (
    SyncFailedError,
    SyncReport,
    render_diff,
    render_planned_sql,
    render_report,
)
from delta_engine.cli.connection import (
    ConnectionSettings,
    open_connection,
    resolve_connection_settings,
)
from delta_engine.cli.declarations import load_declarations
from delta_engine.cli.errors import ConfigError, DeclarationImportError
from delta_engine.databricks import build_sql_engine, configure_logging

if TYPE_CHECKING:
    from databricks.sql.client import Connection

app = typer.Typer(
    name="delta-engine",
    help="Declarative schema management for Delta Lake tables on Databricks.",
    no_args_is_help=True,
)


class OutputFormat(StrEnum):
    """Report formats the CLI can emit."""

    TEXT = "text"
    JSON = "json"


_EXIT_IN_SYNC = 0
_EXIT_FAILURES = 1
_EXIT_CHANGES_PENDING = 2

SpecsArgument = Annotated[
    list[str],
    typer.Argument(
        metavar="MODULE[:ATTR]...",
        help="Declaration modules, e.g. myproject.tables or myproject.tables:all_tables.",
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
ServerHostnameOption = Annotated[
    str | None,
    typer.Option("--server-hostname", help="Overrides DATABRICKS_SERVER_HOSTNAME."),
]
HttpPathOption = Annotated[
    str | None, typer.Option("--http-path", help="Overrides DATABRICKS_HTTP_PATH.")
]
VerboseOption = Annotated[
    bool, typer.Option("--verbose", "-v", help="Show engine progress (INFO) on stderr.")
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
    server_hostname: ServerHostnameOption = None,
    http_path: HttpPathOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Dry-run declarations against the catalog; exit 2 when changes are pending."""
    with _anticipated_errors():
        report = _sync(specs, server_hostname, http_path, verbose, dry_run=True)
        _emit(report, output, show_sql, include_diff=True)
        raise typer.Exit(code=_plan_exit_code(report))


@app.command()
def apply(
    specs: SpecsArgument,
    output: OutputOption = OutputFormat.TEXT,
    show_sql: ShowSqlOption = False,
    server_hostname: ServerHostnameOption = None,
    http_path: HttpPathOption = None,
    verbose: VerboseOption = False,
) -> None:
    """Sync declarations to the catalog; exit 1 when any table fails."""
    with _anticipated_errors():
        try:
            report = _sync(specs, server_hostname, http_path, verbose, dry_run=False)
        except SyncFailedError as error:
            _emit(error.report, output, show_sql, include_diff=False)
            raise typer.Exit(code=_EXIT_FAILURES) from None
        _emit(report, output, show_sql, include_diff=False)
        raise typer.Exit(code=_EXIT_IN_SYNC)


def _plan_exit_code(report: SyncReport) -> int:
    if report.has_failures:
        return _EXIT_FAILURES
    if report.has_changes:
        return _EXIT_CHANGES_PENDING
    return _EXIT_IN_SYNC


def _sync(
    spec_texts: list[str],
    server_hostname: str | None,
    http_path: str | None,
    verbose: bool,
    *,
    dry_run: bool,
) -> SyncReport:
    """Load declarations, open the connection, and run one sync."""
    level = logging.INFO if verbose else logging.WARNING
    # sys.stderr (not the sys.__stderr__ default) so ordinary stream
    # redirection — including test runners — captures engine logs.
    configure_logging(level=level, stream=sys.stderr)
    tables = load_declarations(spec_texts)
    settings = resolve_connection_settings(server_hostname, http_path, os.environ)
    connection = _connect(settings)
    try:
        engine = build_sql_engine(connection)
        try:
            return engine.sync(*tables, dry_run=dry_run)
        except ValueError as error:
            # prepare_desired_tables rejects duplicate qualified names before
            # any phase runs; that is a declaration problem, not a bug.
            raise ConfigError(str(error)) from error
    finally:
        connection.close()


def _connect(settings: ConnectionSettings) -> "Connection":
    try:
        return open_connection(settings)
    except ConfigError:
        raise
    except Exception as error:
        raise ConfigError(
            f"failed to connect to Databricks ({type(error).__name__}): {error}; "
            "check the DATABRICKS_* settings"
        ) from error


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
    """Print anticipated failures as messages, user bugs as tracebacks; exit 1."""
    try:
        yield
    except ConfigError as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(code=_EXIT_FAILURES) from None
    except DeclarationImportError as error:
        typer.echo(f"error: {error}", err=True)
        if error.__cause__ is not None:
            traceback.print_exception(error.__cause__, file=sys.stderr)
        raise typer.Exit(code=_EXIT_FAILURES) from None
