"""Shared CLI test fixtures: fake engine boundary and declaration modules."""

import sys
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from delta_engine.application.engine import Engine
from delta_engine.application.ports import (
    CatalogState,
    ExecutionSucceeded,
    ExecutionSummary,
    TableAbsent,
)
import delta_engine.cli.app as cli_app
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan


class FakeReader:
    """Catalog reader answering from a fixed mapping; absent by default."""

    def __init__(self, states: dict[str, CatalogState] | None = None) -> None:
        self.states = states or {}

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        return self.states.get(str(qualified_name), TableAbsent())


class FakeExecutor:
    """Executor that compiles one pseudo-statement per action and always succeeds."""

    def compile(self, qualified_name: QualifiedName, plan: ActionPlan) -> tuple[str, ...]:
        return tuple(f"-- {qualified_name}: {type(action).__name__}" for action in plan)

    def execute(self, statements: tuple[str, ...]) -> ExecutionSummary:
        return ExecutionSummary(
            results=tuple(
                ExecutionSucceeded(statement_index=index, statement=statement)
                for index, statement in enumerate(statements)
            )
        )


class _StubConnection:
    def close(self) -> None:
        pass


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def fake_engine(monkeypatch):
    """Route the CLI's engine boundary to fakes; yield the reader to preload states."""
    reader = FakeReader()
    engine = Engine(reader=reader, executor=FakeExecutor())
    monkeypatch.setattr(cli_app, "open_connection", lambda settings: _StubConnection())
    monkeypatch.setattr(cli_app, "build_sql_engine", lambda connection: engine)
    return reader


@pytest.fixture
def databricks_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "test.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/test")
    monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")


@pytest.fixture
def write_module(tmp_path, monkeypatch):
    """Write a declarations module importable by the CLI; clean up sys.modules."""
    monkeypatch.syspath_prepend(str(tmp_path))
    created: list[str] = []

    def _write(module_name: str, source: str) -> str:
        (tmp_path / f"{module_name}.py").write_text(dedent(source))
        created.append(module_name)
        return module_name

    yield _write
    for name in created:
        sys.modules.pop(name, None)


ORDERS_ONLY = """
    from delta_engine.schema import Column, DeltaTable, String

    orders = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
"""
