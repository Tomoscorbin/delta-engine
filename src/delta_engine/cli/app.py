"""The ``delta-engine`` plan, apply, generate, and lint commands."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from enum import StrEnum
import json
import logging
from pathlib import Path
import sys
import tomllib
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
from delta_engine.cli.rendering import render_lint, render_sync
from delta_engine.databricks import build_reader, build_sql_engine
from delta_engine.domain.model import QualifiedName
from delta_engine.lint import LintConfigError, LintReport, lint_tables, parse_lint_config

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
_DEFAULT_CONFIG_PATH = Path("pyproject.toml")

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

LintDeclarationArgument = Annotated[
    str | None,
    typer.Argument(
        metavar="MODULE:ATTRIBUTE",
        help="Declarations to lint; defaults to 'declarations' in [tool.delta-engine.lint].",
    ),
]

LintConfigOption = Annotated[
    Path | None,
    typer.Option(
        "--config",
        help="TOML file carrying [tool.delta-engine.lint]; defaults to ./pyproject.toml.",
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
        raise typer.Exit(
            code=_resolve_plan_exit_code(result.report, fail_on_changes=fail_on_changes)
        )


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


def _resolve_plan_exit_code(report: SyncReport, *, fail_on_changes: bool) -> int:
    """Map one dry-run report to the plan command's exit code."""
    if report.has_failures:
        return _EXIT_FAILURE
    if fail_on_changes and report.has_changes:
        return _EXIT_FAILURE
    return _EXIT_SUCCESS


@app.command()
def lint(
    declaration: LintDeclarationArgument = None,
    output: OutputOption = OutputFormat.TEXT,
    config: LintConfigOption = None,
) -> None:
    """Check declarations against the configured lint rules; never opens a connection."""
    with _anticipated_errors():
        section = _load_lint_section(config)
        policy = parse_lint_config(section)
        reference = _resolve_lint_target(declaration, section)
        with _engine_logging(), redirect_stdout(sys.stderr):
            tables = load_declarations(reference)
        report = lint_tables(*tables, policy=policy)
        typer.echo(_render_lint_report(report, output))
        raise typer.Exit(code=_EXIT_FAILURE if report.has_errors else _EXIT_SUCCESS)


def _load_lint_section(path: Path | None) -> Mapping[str, object]:
    """Read ``[tool.delta-engine.lint]``; a missing default file means an empty section."""
    if path is None and not _DEFAULT_CONFIG_PATH.exists():
        return {}
    selected = path if path is not None else _DEFAULT_CONFIG_PATH
    try:
        content = selected.read_text()
    except OSError as error:
        raise ConfigError(f"cannot read config file {selected}: {error}") from None
    try:
        data = tomllib.loads(content)
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"invalid TOML in {selected}: {error}") from None
    return _lint_section(data, selected)


def _lint_section(data: Mapping[str, object], path: Path) -> Mapping[str, object]:
    """Select ``[tool.delta-engine.lint]``; an absent table means an empty section."""
    section: Mapping[str, object] = data
    for key in ("tool", "delta-engine", "lint"):
        value = section.get(key, {})
        if not isinstance(value, Mapping):
            raise ConfigError(f"'{key}' in {path} must be a TOML table")
        section = value
    return section


def _resolve_lint_target(argument: str | None, section: Mapping[str, object]) -> DeclarationRef:
    """Pick the declarations to lint: the argument, else the configured target."""
    target = argument if argument is not None else section.get("declarations")
    if target is None:
        raise ConfigError(
            "no declarations given; pass MODULE:ATTRIBUTE or set"
            " 'declarations' in [tool.delta-engine.lint]"
        )
    if not isinstance(target, str):
        raise ConfigError(f"declarations: expected a 'module:attribute' string, got {target!r}")
    return DeclarationRef.parse(target)


def _render_lint_report(report: LintReport, output: OutputFormat) -> str:
    """Render one lint report as the selected stdout report format."""
    if output is OutputFormat.JSON:
        return json.dumps(report.to_dict(), indent=2)
    return render_lint(report)


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
    except (
        ConfigError,
        DuplicateTableDefinitionError,
        GenerationError,
        LintConfigError,
        ReadError,
    ) as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(code=_EXIT_FAILURE) from None
