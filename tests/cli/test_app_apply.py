"""Behaviour of `delta-engine apply`: execution, failure rendering, user-bug tracebacks."""

from contextlib import contextmanager
import json

import delta_engine.cli.app as cli_app
from delta_engine.cli.app import app
from tests.cli.conftest import NOT_NULL_DRIFT_ORDERS, ORDERS_ONLY, observed_orders

RAISES_ON_IMPORT = """
    raise RuntimeError("boom in user code")
"""

DUPLICATE_ORDERS = """
    from delta_engine.schema import Column, DeltaTable, String

    orders_a = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    orders_b = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    all_tables = [orders_a, orders_b]
"""


def test_apply_executes_pending_changes_and_exits_zero(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_creates", ORDERS_ONLY)

    result = runner.invoke(app, ["apply", f"{module}:orders"])

    assert result.exit_code == 0
    assert "SYNC REPORT" in result.stdout
    assert "DRY RUN" not in result.stdout
    assert "1/1" in result.stdout  # applied/planned statement count in the grid


def test_apply_is_a_no_op_when_already_in_sync(runner, fake_engine, databricks_env, write_module):
    module = write_module("apply_in_sync", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["apply", f"{module}:orders"])

    assert result.exit_code == 0
    assert "no changes" in result.stdout


def test_apply_renders_the_failure_report_and_exits_one(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration whose drift fails validation (NOT NULL add on existing table)
    module = write_module("apply_invalid", NOT_NULL_DRIFT_ORDERS)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["apply", f"{module}:orders"])

    assert result.exit_code == 1
    assert "Failures" in result.stdout


def test_apply_failure_in_json_mode_emits_the_report_payload(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_invalid_json", NOT_NULL_DRIFT_ORDERS)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["apply", f"{module}:orders", "--output", "json"])

    payload = json.loads(result.stdout)
    assert result.exit_code == 1
    assert payload["has_failures"] is True
    assert payload["tables"][0]["status"] == "VALIDATION_FAILED"


def test_a_module_that_raises_shows_the_users_traceback(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_user_bug", RAISES_ON_IMPORT)

    result = runner.invoke(app, ["apply", f"{module}:orders"])

    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "boom in user code"
    assert result.exception.__cause__ is None


def test_duplicate_qualified_names_are_a_config_error(
    runner, fake_engine, databricks_env, write_module, monkeypatch
):
    module = write_module("apply_duplicates", DUPLICATE_ORDERS)

    connection_was_opened = False

    @contextmanager
    def unexpected_connection(host, http_path, profile):
        nonlocal connection_was_opened
        connection_was_opened = True
        yield object()

    monkeypatch.setattr(cli_app, "open_connection", unexpected_connection)
    result = runner.invoke(app, ["apply", f"{module}:all_tables"])

    assert result.exit_code == 1
    assert "orders" in result.stderr
    assert "Traceback" not in result.stderr
    assert connection_was_opened is False


def test_unexpected_sync_error_propagates_after_connection_cleanup(
    runner, fake_engine, databricks_env, write_module, monkeypatch
):
    module = write_module("apply_compiler_bug", ORDERS_ONLY)
    state = {"closed": False}

    @contextmanager
    def recording_connection(host, http_path, profile):
        try:
            yield object()
        finally:
            state["closed"] = True

    class BrokenEngine:
        def sync(self, *tables, dry_run=False):
            raise RuntimeError("compiler defect")

    monkeypatch.setattr(cli_app, "open_connection", recording_connection)
    monkeypatch.setattr(cli_app, "build_sql_engine", lambda connection: BrokenEngine())

    result = runner.invoke(app, ["apply", f"{module}:orders"])

    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "compiler defect"
    assert state["closed"] is True
