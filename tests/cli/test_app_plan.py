"""Behaviour of `delta-engine plan`: exit codes, text and JSON output."""

from contextlib import contextmanager
import json
import logging

import delta_engine.cli.app as cli_app
from delta_engine.cli.app import app
from delta_engine.cli.connection import open_connection as real_open_connection
from tests.cli.conftest import NOT_NULL_DRIFT_ORDERS, ORDERS_ONLY, observed_orders


def test_plan_exits_zero_when_catalog_matches_declarations(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_in_sync", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["plan", f"{module}:orders"])

    assert result.exit_code == 0
    assert "no changes" in result.stdout


def test_plan_valid_pending_changes_exit_zero_and_show_the_diff_by_default(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_drift", ORDERS_ONLY)

    result = runner.invoke(app, ["plan", f"{module}:orders"])

    assert result.exit_code == 0
    assert "DIFF" in result.stdout
    assert "SYNC REPORT" in result.stdout
    assert "DRY RUN" in result.stdout
    assert "dev.silver.orders" in result.stdout


def test_plan_exits_two_for_pending_changes_only_with_fail_on_changes(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_drift_gate", ORDERS_ONLY)

    result = runner.invoke(
        app,
        ["plan", f"{module}:orders", "--fail-on-changes"],
    )

    assert result.exit_code == 2
    assert "dev.silver.orders" in result.stdout


def test_plan_exits_one_when_validation_fails(runner, fake_engine, databricks_env, write_module):
    # Given a declaration adding a NOT NULL column to an existing table
    module = write_module("plan_invalid", NOT_NULL_DRIFT_ORDERS)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["plan", f"{module}:orders"])

    assert result.exit_code == 1
    assert "VALIDATION_FAILED" in result.stdout


def test_plan_json_stdout_is_pure_and_matches_the_report_schema(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_json", ORDERS_ONLY)

    result = runner.invoke(app, ["plan", f"{module}:orders", "--output", "json"])

    payload = json.loads(result.stdout)  # parses, so nothing else is on stdout
    assert result.exit_code == 0
    assert payload["schema_version"] == 1
    assert payload["dry_run"] is True
    assert payload["has_changes"] is True
    assert payload["tables"][0]["name"] == "dev.silver.orders"


def test_plan_show_sql_appends_the_planned_statements(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_show_sql", ORDERS_ONLY)

    result = runner.invoke(app, ["plan", f"{module}:orders", "--show-sql"])

    assert result.exit_code == 0
    assert "PLANNED SQL" in result.stdout
    assert "-- dev.silver.orders" in result.stdout


def test_plan_reports_missing_connection_settings_without_a_traceback(
    runner, fake_engine, write_module, monkeypatch
):
    monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)
    monkeypatch.setattr(cli_app, "open_connection", real_open_connection)
    module = write_module("plan_no_env", ORDERS_ONLY)

    result = runner.invoke(app, ["plan", f"{module}:orders"])

    assert result.exit_code == 1
    assert "DATABRICKS_HTTP_PATH" in result.stderr
    assert "Traceback" not in result.stderr


def test_plan_reports_a_missing_module_as_a_config_error(runner, fake_engine, databricks_env):
    result = runner.invoke(app, ["plan", "does.not.exist:tables"])

    assert result.exit_code == 1
    assert "does.not.exist" in result.stderr
    assert "Traceback" not in result.stderr


def test_json_redirects_declaration_authentication_and_sync_stdout_to_stderr(
    runner, fake_engine, databricks_env, write_module, monkeypatch
):
    module = write_module(
        "plan_json_noise",
        """
        print("declaration noise")
        from delta_engine.schema import Column, DeltaTable, String

        orders = DeltaTable(
            "dev", "silver", "orders", columns=(Column("id", String()),)
        )
        """,
    )
    original_connection = cli_app.open_connection
    original_build = cli_app.build_sql_engine

    @contextmanager
    def noisy_connection(host, http_path, profile):
        print("authentication noise")
        with original_connection(host, http_path, profile) as connection:
            yield connection

    def noisy_build(connection):
        print("sync noise")
        return original_build(connection)

    monkeypatch.setattr(cli_app, "open_connection", noisy_connection)
    monkeypatch.setattr(cli_app, "build_sql_engine", noisy_build)

    result = runner.invoke(
        app,
        ["plan", f"{module}:orders", "--output", "json"],
    )

    payload = json.loads(result.stdout)
    assert payload["has_changes"] is True
    assert "declaration noise" in result.stderr
    assert "authentication noise" in result.stderr
    assert "sync noise" in result.stderr


def test_click_usage_errors_keep_exit_code_two(runner):
    result = runner.invoke(app, ["plan"])

    assert result.exit_code == 2
    assert "Missing argument" in result.stderr


def test_version_prints_the_package_version(runner):
    import delta_engine

    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert delta_engine.__version__ in result.stdout


def test_engine_logging_handlers_do_not_leak_across_invocations(
    runner, fake_engine, databricks_env, write_module
):
    # Given the handler state before any CLI invocation
    package_logger = logging.getLogger("delta_engine")
    root_logger = logging.getLogger()
    package_handlers_before = list(package_logger.handlers)
    root_handlers_before = list(root_logger.handlers)
    module = write_module("plan_logging", ORDERS_ONLY)

    # When running a command (whose invocation-scoped stderr stream then closes)
    runner.invoke(app, ["plan", f"{module}:orders"])

    # Then no handler bound to the dead stream is left behind, so later
    # logging in the same process cannot hit a closed file
    assert package_logger.handlers == package_handlers_before
    assert root_logger.handlers == root_handlers_before
    logging.getLogger("delta_engine.cli").warning("post-invocation log line")
