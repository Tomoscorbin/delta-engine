"""The ``delta-engine`` plan, apply, and generate commands."""

from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from enum import StrEnum
import json
import logging
import sys
from typing import Annotated

import typer

import delta_engine
from delta_engine.api.codegen import GeneratedModule, GenerationError, generate_module
from delta_engine.application import DuplicateTableDefinitionError, SyncReport
from delta_engine.application.errors import ReadError, SyncFailedError
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.cli.connection import Target, open_connection
from delta_engine.cli.declarations import DeclarationRef, load_declarations
from delta_engine.cli.errors import ConfigError
from delta_engine.cli.rendering import render_sync
from delta_engine.databricks import build_reader, build_sql_engine
from delta_engine.domain.model import QualifiedName

app = typer.Typer(
    name="delta-engine",
    help="Declarative schema plans and applies for Delta Lake tables on Databricks.",
    no_args_is_help=True,
    add_completion=False,
    # Plain tracebacks keep local values out of CI logs.
    pretty_exceptions_enable=False,
)

_EXIT_SUCCESS = 0
_EXIT_FAILURE = 1

DeclarationArgument = Annotated[
    str,
    typer.Argument(
        metavar="MODULE:ATTRIBUTE",
        help="One attribute containing a non-empty ordered sequence of DeltaTable declarations.",
    ),
]

TableNameArgument = Annotated[
    str,
    typer.Argument(
        metavar="CATALOG.SCHEMA.TABLE",
        help="Fully qualified name of one live Unity Catalog table.",
    ),
]


class OutputFormat(StrEnum):
    """Report formats the CLI can print on stdout."""

    TEXT = "text"
    JSON = "json"


OutputOption = Annotated[
    OutputFormat,
    typer.Option("--output", help="Report format on stdout."),
]

FailOnChangesOption = Annotated[
    bool,
    typer.Option(
        "--fail-on-changes",
        help="Exit 1 when a valid plan contains pending changes.",
    ),
]


@dataclass(frozen=True, slots=True)
class SyncView:
    """The safe-to-render identity and report a sync command displays."""

    target: Target
    declaration: DeclarationRef
    report: SyncReport


def _version_callback(value: bool) -> None:
    """Print the installed package version for the eager global option."""
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
    """Handle the global ``--version`` option."""


@app.command()
def plan(
    declaration: DeclarationArgument,
    output: OutputOption = OutputFormat.TEXT,
    fail_on_changes: FailOnChangesOption = False,
) -> None:
    """Read the live catalog and print a plan; never execute planned DDL."""
    with _anticipated_errors():
        reference = DeclarationRef.parse(declaration)
        result = _sync(reference, dry_run=True)
        typer.echo(_render_sync_view(result, output))
        raise typer.Exit(code=_plan_exit_code(result.report, fail_on_changes=fail_on_changes))


@app.command()
def apply(
    declaration: DeclarationArgument,
    output: OutputOption = OutputFormat.TEXT,
) -> None:
    """Read the live catalog and execute the planned DDL; unsafe plans are rejected."""
    with _anticipated_errors():
        reference = DeclarationRef.parse(declaration)
        result = _sync(reference, dry_run=False)
        typer.echo(_render_sync_view(result, output))
        raise typer.Exit(code=_EXIT_FAILURE if result.report.has_failures else _EXIT_SUCCESS)


def _sync(reference: DeclarationRef, *, dry_run: bool) -> SyncView:
    """Load one collection, authenticate, and run one engine sync."""
    with _engine_logging(), redirect_stdout(sys.stderr):
        tables = load_declarations(reference)
        with open_connection() as (target, connection):
            engine = build_sql_engine(connection)
            try:
                report = engine.sync(*tables, dry_run=dry_run)
            except SyncFailedError as error:
                report = error.report
    return SyncView(target=target, declaration=reference, report=report)


def _render_sync_view(view: SyncView, output: OutputFormat) -> str:
    """Render one sync view as the selected stdout report format."""
    if output is OutputFormat.JSON:
        return json.dumps(view.report.to_dict(), indent=2)
    return render_sync(view.target, view.declaration, view.report)


def _plan_exit_code(report: SyncReport, *, fail_on_changes: bool) -> int:
    """Map one dry-run report to the plan command's exit code."""
    if report.has_failures:
        return _EXIT_FAILURE
    if fail_on_changes and report.has_changes:
        return _EXIT_FAILURE
    return _EXIT_SUCCESS


@app.command()
def generate(table_name: TableNameArgument) -> None:
    """Read one live table and print an importable DeltaTable declaration."""
    with _anticipated_errors():
        qualified_name = _parse_table_name(table_name)
        module = _generate(qualified_name)
        for warning in module.warnings:
            typer.echo(f"warning: {warning}", err=True)
        typer.echo(module.source, nl=False)


def _parse_table_name(text: str) -> QualifiedName:
    """Parse one fully qualified table name into its domain form."""
    try:
        return QualifiedName.parse(text)
    except ValueError as error:
        raise ConfigError(str(error)) from None


def _generate(qualified_name: QualifiedName) -> GeneratedModule:
    """Read one table's live state and raise it into a declaration module."""
    with _engine_logging(), redirect_stdout(sys.stderr):
        with open_connection() as (_target, connection):
            state = build_reader(connection).fetch_state(qualified_name)
    match state:
        case TablePresent(table=observed):
            return generate_module(observed)
        case TableAbsent():
            raise ConfigError(f"table {qualified_name} does not exist")


@contextmanager
def _engine_logging() -> Iterator[None]:
    """Attach one invocation-scoped stderr handler to the package logger."""
    package_logger = logging.getLogger("delta_engine")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s | %(name)s | %(message)s"))
    previous_level = package_logger.level
    previous_propagate = package_logger.propagate
    package_logger.addHandler(handler)
    package_logger.setLevel(logging.WARNING)
    package_logger.propagate = False
    try:
        yield
    finally:
        package_logger.removeHandler(handler)
        package_logger.setLevel(previous_level)
        package_logger.propagate = previous_propagate


@contextmanager
def _anticipated_errors() -> Iterator[None]:
    """Render expected configuration failures; let code defects propagate."""
    try:
        yield
    except (ConfigError, DuplicateTableDefinitionError, GenerationError, ReadError) as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(code=_EXIT_FAILURE) from None
