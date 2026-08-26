"""Shared CLI test fixtures: fake engine boundary and declaration modules."""

from contextlib import contextmanager
import sys
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from delta_engine.application.engine import Engine
from delta_engine.application.errors import ExecutionError, ReadError
from delta_engine.application.ports import (
    CatalogState,
    CompiledAction,
    CompiledPlan,
    TableAbsent,
    TablePresent,
)
import delta_engine.cli.app as cli_app
from delta_engine.cli.connection import Target
from delta_engine.domain.model import (
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.plan import ActionPlan


def observed_orders() -> TablePresent:
    """Return an observed dev.silver.orders with one nullable id column."""
    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName("dev", "silver", "orders"),
            columns=(ObservedColumn("id", String()),),
        )
    )


class FakeReader:
    """Catalog reader answering from a fixed mapping; absent by default."""

    def __init__(self, states: dict[str, CatalogState | ReadError] | None = None) -> None:
        self.states = states or {}

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        result = self.states.get(str(qualified_name), TableAbsent())
        if isinstance(result, ReadError):
            raise result
        return result


class FakeExecutor:
    """Executor that compiles one pseudo-statement per action and always succeeds."""

    def compile(self, plan: ActionPlan) -> CompiledPlan:
        return CompiledPlan(
            plan=plan,
            compiled_actions=tuple(
                CompiledAction(
                    action=action,
                    statement=f"-- {plan.target}: {type(action).__name__}",
                )
                for action in plan
            ),
        )

    def execute(self, statement: str) -> None:
        pass


class FailingExecutor(FakeExecutor):
    """Executor whose every statement fails at the backend."""

    def execute(self, statement: str) -> None:
        raise ExecutionError("PermissionDenied", "cannot modify table")


class _StubConnection:
    def close(self) -> None:
        pass


@contextmanager
def _open_fake_connection():
    yield (
        Target(
            host="https://test.cloud.databricks.com",
            warehouse_id="test-warehouse",
        ),
        _StubConnection(),
    )


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def fake_engine(monkeypatch):
    """Route the CLI's engine boundary to fakes; yield the reader to preload states."""
    reader = FakeReader()
    engine = Engine(reader=reader, executor=FakeExecutor())
    monkeypatch.setattr(cli_app, "open_connection", _open_fake_connection)
    monkeypatch.setattr(cli_app, "build_sql_engine", lambda connection: engine)
    return reader


@pytest.fixture
def failing_engine(monkeypatch):
    """Route the CLI's engine boundary to an engine whose executor always fails."""
    reader = FakeReader()
    engine = Engine(reader=reader, executor=FailingExecutor())
    monkeypatch.setattr(cli_app, "open_connection", _open_fake_connection)
    monkeypatch.setattr(cli_app, "build_sql_engine", lambda connection: engine)
    return reader


@pytest.fixture
def fake_reader(monkeypatch):
    """Route the CLI's reader boundary to a fake; yield it to preload states."""
    reader = FakeReader()
    monkeypatch.setattr(cli_app, "open_connection", _open_fake_connection)
    monkeypatch.setattr(cli_app, "build_reader", lambda connection: reader)
    return reader


@pytest.fixture
def databricks_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_SQL_WAREHOUSE_ID", "test-warehouse")


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
    all_tables = [orders]
"""

# Declares a NOT NULL column addition, which fails validation when diffed
# against observed_orders().
NOT_NULL_DRIFT_ORDERS = """
    from delta_engine.schema import Column, DeltaTable, String

    orders = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(
            Column("id", String()),
            Column("amount", String(), nullable=False),
        ),
    )
    all_tables = [orders]
"""
