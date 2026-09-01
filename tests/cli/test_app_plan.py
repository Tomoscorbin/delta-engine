"""Behaviour of the single read-only ``delta-engine plan`` workflow."""

from contextlib import contextmanager
import json
import logging

import pytest

from delta_engine.application.errors import ReadError
import delta_engine.cli.app as cli_app
from delta_engine.cli.app import app
from delta_engine.cli.connection import Target, open_connection as real_open_connection
from tests.cli.conftest import NOT_NULL_DRIFT_ORDERS, ORDERS_ONLY, observed_orders

RAISES_ON_IMPORT = 'raise RuntimeError("boom in user code")\n'

DUPLICATE_ORDERS = """
    from delta_engine.schema import Column, DeltaTable, String

    orders_a = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    orders_b = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    all_tables = [orders_a, orders_b]
"""


def test_in_sync_plan_exits_zero_and_prints_the_complete_identity_and_report(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration matching the live catalog
    module = write_module("plan_in_sync", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then the run succeeds, every identity and report section prints, the
    # heading states nothing executed, and no SQL section appears
    assert result.exit_code == 0
    assert "TARGET" in result.stdout
    assert "Host: https://test.cloud.databricks.com" in result.stdout
    assert "SQL warehouse: test-warehouse" in result.stdout
    assert f"Declarations: {module}:all_tables" in result.stdout
    assert "DIFF" in result.stdout
    assert "SYNC REPORT" in result.stdout
    assert "PLAN — no planned SQL executed" in result.stdout
    assert "PLANNED SQL" not in result.stdout


def test_changed_plan_exits_zero_and_prints_diff_report_and_sql_in_order(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration that drifts from the live catalog
    module = write_module("plan_drift", ORDERS_ONLY)

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then a pending change still exits zero and the sections print in
    # identity, diff, report, SQL order
    assert result.exit_code == 0
    assert "dev.silver.orders" in result.stdout
    assert "-- dev.silver.orders: CreateTable" in result.stdout
    assert result.stdout.index("TARGET") < result.stdout.index("DIFF")
    assert result.stdout.index("DIFF") < result.stdout.index("SYNC REPORT")
    assert result.stdout.index("SYNC REPORT") < result.stdout.index("PLANNED SQL")


def test_fail_on_changes_exits_one_when_changes_are_pending(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration that drifts from the live catalog
    module = write_module("plan_gate_drift", ORDERS_ONLY)

    # When planning with the drift gate enabled
    result = runner.invoke(app, ["plan", f"{module}:all_tables", "--fail-on-changes"])

    # Then the full report still prints and the exit code signals drift
    assert result.exit_code == 1
    assert "PLANNED SQL" in result.stdout


def test_fail_on_changes_exits_zero_when_in_sync(runner, fake_engine, databricks_env, write_module):
    # Given a declaration matching the live catalog
    module = write_module("plan_gate_in_sync", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    # When planning with the drift gate enabled
    result = runner.invoke(app, ["plan", f"{module}:all_tables", "--fail-on-changes"])

    # Then the plan succeeds
    assert result.exit_code == 0


def test_fail_on_changes_with_json_output_still_prints_the_report(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration that drifts from the live catalog
    module = write_module("plan_gate_json_drift", ORDERS_ONLY)

    # When planning with the drift gate and JSON output together
    result = runner.invoke(
        app,
        ["plan", f"{module}:all_tables", "--output", "json", "--fail-on-changes"],
    )

    # Then the machine-readable report still reaches stdout before the drift exit
    assert result.exit_code == 1
    report = json.loads(result.stdout)
    assert report["has_changes"] is True
    assert report["has_failures"] is False


def test_json_output_prints_the_versioned_run_report_alone(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration that drifts from the live catalog
    module = write_module("plan_json_drift", ORDERS_ONLY)

    # When planning with JSON output
    result = runner.invoke(app, ["plan", f"{module}:all_tables", "--output", "json"])

    # Then stdout is exactly one machine-readable run report
    assert result.exit_code == 0
    report = json.loads(result.stdout)
    assert report["schema_version"] == 2
    assert report["dry_run"] is True
    assert report["has_changes"] is True
    assert report["has_failures"] is False
    assert [table["name"] for table in report["tables"]] == ["dev.silver.orders"]
    assert "TARGET" not in result.stdout


def test_json_output_with_failures_is_parseable_and_exits_one(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration whose diff fails validation
    module = write_module("plan_json_invalid", NOT_NULL_DRIFT_ORDERS)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    # When planning with JSON output
    result = runner.invoke(app, ["plan", f"{module}:all_tables", "--output", "json"])

    # Then the report is still valid JSON and the exit code still signals failure
    assert result.exit_code == 1
    report = json.loads(result.stdout)
    assert report["has_failures"] is True
    assert report["tables"][0]["status"] == "PLANNING_FAILED"


def test_unknown_output_format_is_a_usage_error(runner):
    # Given an output format the CLI does not offer
    # When invoking plan with it
    result = runner.invoke(app, ["plan", "some.module:tables", "--output", "yaml"])

    # Then the framework rejects the usage before any work starts
    assert result.exit_code == 2


def test_validation_failure_prints_the_plan_report_and_exits_one(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration whose diff fails validation
    module = write_module("plan_invalid", NOT_NULL_DRIFT_ORDERS)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then the full report still prints, names the failure, and exits one
    assert result.exit_code == 1
    assert "TARGET" in result.stdout
    assert "DIFF" in result.stdout
    assert "PLANNING_FAILED" in result.stdout
    assert "Failures" in result.stdout


def test_catalog_read_failure_prints_the_plan_report_and_exits_one(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declared table whose catalog state cannot be read
    module = write_module("plan_read_failure", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = ReadError("PermissionDenied", "cannot inspect table")

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then the report shows the read failure and the command exits one
    assert result.exit_code == 1
    assert "READ_FAILED" in result.stdout
    assert "PermissionDenied" in result.stdout


def test_missing_warehouse_setting_is_a_one_line_configuration_error(
    runner, fake_engine, write_module, monkeypatch
):
    # Given an environment without the warehouse setting and the real
    # connection boundary in place
    monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
    monkeypatch.setattr(cli_app, "open_connection", real_open_connection)
    module = write_module("plan_no_env", ORDERS_ONLY)

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then stderr carries one error line naming the setting, with no traceback
    assert result.exit_code == 1
    assert "DATABRICKS_SQL_WAREHOUSE_ID" in result.stderr
    assert result.stderr.count("\n") == 1
    assert "Traceback" not in result.stderr


def test_missing_module_is_a_configuration_error(runner, fake_engine, databricks_env):
    # When planning against a module that cannot be imported
    result = runner.invoke(app, ["plan", "does.not.exist:all_tables"])

    # Then the error names the module on stderr with no traceback
    assert result.exit_code == 1
    assert "does.not.exist" in result.stderr
    assert "Traceback" not in result.stderr


def test_import_authentication_and_sync_stdout_are_redirected_to_stderr(
    runner, fake_engine, databricks_env, write_module, monkeypatch
):
    # Given declaration import, authentication, and sync that all print noise
    module = write_module(
        "plan_noise",
        """
        print("declaration noise")
        from delta_engine.schema import Column, DeltaTable, String

        orders = DeltaTable(
            "dev", "silver", "orders", columns=(Column("id", String()),)
        )
        all_tables = [orders]
        """,
    )
    original_connection = cli_app.open_connection
    original_build = cli_app.build_sql_engine

    @contextmanager
    def noisy_connection():
        print("authentication noise")
        with original_connection() as connected:
            yield connected

    class NoisyEngine:
        def __init__(self, engine):
            self.engine = engine

        def sync(self, *tables, dry_run=False):
            print("sync noise")
            return self.engine.sync(*tables, dry_run=dry_run)

    def noisy_build(connection):
        return NoisyEngine(original_build(connection))

    monkeypatch.setattr(cli_app, "open_connection", noisy_connection)
    monkeypatch.setattr(cli_app, "build_sql_engine", noisy_build)

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then every noise line lands on stderr, keeping stdout parseable
    assert result.exit_code == 0
    assert "declaration noise" not in result.stdout
    assert "authentication noise" not in result.stdout
    assert "sync noise" not in result.stdout
    assert "declaration noise" in result.stderr
    assert "authentication noise" in result.stderr
    assert "sync noise" in result.stderr


def test_declaration_import_exception_retains_the_original_traceback(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration module whose import raises
    module = write_module("plan_user_bug", RAISES_ON_IMPORT)

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then the user's exception escapes unwrapped, keeping its traceback
    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "boom in user code"
    assert result.exception.__cause__ is None


def test_duplicate_names_are_configuration_errors_before_connecting(
    runner, fake_engine, databricks_env, write_module, monkeypatch
):
    # Given duplicate declarations and a connection boundary that records use
    module = write_module("plan_duplicates", DUPLICATE_ORDERS)
    connection_was_opened = False

    @contextmanager
    def unexpected_connection():
        nonlocal connection_was_opened
        connection_was_opened = True
        yield object()

    monkeypatch.setattr(cli_app, "open_connection", unexpected_connection)

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then the duplicate fails as configuration before any connection opens
    assert result.exit_code == 1
    assert "Duplicate table definition: dev.silver.orders" in result.stderr
    assert connection_was_opened is False


def test_unexpected_engine_error_propagates_after_connection_cleanup(
    runner, fake_engine, databricks_env, write_module, monkeypatch
):
    # Given an engine that raises a code defect mid-sync
    module = write_module("plan_compiler_bug", ORDERS_ONLY)
    state = {"closed": False}

    @contextmanager
    def recording_connection():
        try:
            yield (
                Target("https://test.cloud.databricks.com", "warehouse"),
                object(),
            )
        finally:
            state["closed"] = True

    class BrokenEngine:
        def sync(self, *tables, dry_run=False):
            assert dry_run is True
            raise RuntimeError("compiler defect")

    monkeypatch.setattr(cli_app, "open_connection", recording_connection)
    monkeypatch.setattr(cli_app, "build_sql_engine", lambda connection: BrokenEngine())

    # When planning
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then the defect propagates unwrapped and the connection still closed
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "compiler defect"
    assert state["closed"] is True


@pytest.mark.parametrize(
    "arguments",
    [
        ["plan", "some.module:tables", "other.module:tables"],
        ["plan", "some.module:tables", "--show-sql"],
        ["plan", "some.module:tables", "--host", "example"],
        ["plan", "some.module:tables", "--http-path", "/sql/x"],
        ["plan", "some.module:tables", "--profile", "default"],
        ["plan", "some.module:tables", "--verbose"],
    ],
)
def test_removed_commands_arguments_and_options_are_usage_errors(runner, arguments):
    # Given an invocation using an argument or option the CLI no longer offers

    # Then the framework rejects the usage outright
    result = runner.invoke(app, arguments)
    assert result.exit_code == 2


def test_malformed_cli_usage_keeps_framework_exit_code_two(runner):
    # When invoking plan without its required argument
    result = runner.invoke(app, ["plan"])

    # Then usage mistakes keep the framework's own exit code, distinct from
    # the CLI's failure exit code of one
    assert result.exit_code == 2
    assert "Missing argument" in result.stderr


def test_connection_configuration_errors_do_not_expose_credentials(
    runner, databricks_env, write_module, monkeypatch
):
    # Given credentials that appear in an SDK configuration error
    from databricks.sdk import core as sdk_core

    class BrokenConfig:
        def __init__(self, **kwargs) -> None:
            raise ValueError("bad token test-access-token and secret test-client-secret")

    monkeypatch.setenv("DATABRICKS_TOKEN", "test-access-token")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setattr(sdk_core, "Config", BrokenConfig)
    monkeypatch.setattr(cli_app, "open_connection", real_open_connection)
    module = write_module("plan_no_credentials", ORDERS_ONLY)

    # When the CLI reports the connection failure
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then it fails cleanly with the credential values redacted
    assert result.exit_code == 1
    combined = result.stdout + result.stderr
    assert "test-access-token" not in combined
    assert "test-client-secret" not in combined
    assert "<redacted>" in result.stderr


def test_engine_logging_state_does_not_leak_across_invocations(
    runner, fake_engine, databricks_env, write_module
):
    # Given the caller's logging state before a CLI invocation
    package_logger = logging.getLogger("delta_engine")
    root_logger = logging.getLogger()
    package_handlers_before = list(package_logger.handlers)
    root_handlers_before = list(root_logger.handlers)
    package_propagate_before = package_logger.propagate
    module = write_module("plan_logging", ORDERS_ONLY)

    # When a successful plan completes
    result = runner.invoke(app, ["plan", f"{module}:all_tables"])

    # Then it reports success and restores the caller's logging state
    assert result.exit_code == 0
    assert package_logger.handlers == package_handlers_before
    assert root_logger.handlers == root_handlers_before
    assert package_logger.propagate is package_propagate_before
    logging.getLogger("delta_engine.cli").warning("post-invocation log line")
