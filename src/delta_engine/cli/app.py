"""The read-only ``delta-engine plan`` command."""

from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
import logging
import sys
from typing import Annotated

import typer

import delta_engine
from delta_engine.application import DuplicateTableDefinitionError, SyncReport
from delta_engine.cli.connection import Target, open_connection
from delta_engine.cli.declarations import DeclarationRef, load_declarations
from delta_engine.cli.errors import ConfigError
from delta_engine.cli.rendering import render_plan
from delta_engine.databricks import build_sql_engine

app = typer.Typer(
    name="delta-engine",
    help="Read-only schema plans for Delta Lake tables on Databricks.",
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


@dataclass(frozen=True, slots=True)
class PlanResult:
    """The safe-to-render identity and report returned by the plan service."""

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
def plan(declaration: DeclarationArgument) -> None:
    """Read the live catalog and print a dry-run plan; never execute planned DDL."""
    with _anticipated_errors():
        reference = DeclarationRef.parse(declaration)
        result = _plan(reference)
        typer.echo(render_plan(result.target, result.declaration, result.report))
        raise typer.Exit(code=_EXIT_FAILURE if result.report.has_failures else _EXIT_SUCCESS)


def _plan(reference: DeclarationRef) -> PlanResult:
    """Load one collection, authenticate, and run one read-only engine sync."""
    with _engine_logging(), redirect_stdout(sys.stderr):
        tables = load_declarations(reference)
        with open_connection() as (target, connection):
            engine = build_sql_engine(connection)
            report = engine.sync(*tables, dry_run=True)
    return PlanResult(target=target, declaration=reference, report=report)


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
    except (ConfigError, DuplicateTableDefinitionError) as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(code=_EXIT_FAILURE) from None
