"""Behaviour of `delta-engine plan`: exit codes, text and JSON output."""

import json

from delta_engine.application.ports import TablePresent
from delta_engine.cli.app import app
from delta_engine.domain.model import Column, ObservedTable, QualifiedName, String
from tests.cli.conftest import ORDERS_ONLY

DRIFTING_ORDERS = """
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
"""


def _observed_orders() -> TablePresent:
    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName("dev", "silver", "orders"),
            columns=(Column("id", String()),),
        )
    )


def test_plan_exits_zero_when_catalog_matches_declarations(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_in_sync", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = _observed_orders()

    result = runner.invoke(app, ["plan", module])

    assert result.exit_code == 0
    assert "no changes" in result.stdout


def test_plan_exits_two_and_shows_the_diff_when_changes_are_pending(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_drift", ORDERS_ONLY)

    result = runner.invoke(app, ["plan", module])

    assert result.exit_code == 2
    assert "DIFF" in result.stdout
    assert "SYNC REPORT" in result.stdout
    assert "DRY RUN" in result.stdout
    assert "dev.silver.orders" in result.stdout


def test_plan_exits_one_when_validation_fails(runner, fake_engine, databricks_env, write_module):
    # Given a declaration adding a NOT NULL column to an existing table
    module = write_module("plan_invalid", DRIFTING_ORDERS)
    fake_engine.states["dev.silver.orders"] = _observed_orders()

    result = runner.invoke(app, ["plan", module])

    assert result.exit_code == 1
    assert "VALIDATION_FAILED" in result.stdout


def test_plan_json_stdout_is_pure_and_matches_the_report_schema(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_json", ORDERS_ONLY)

    result = runner.invoke(app, ["plan", module, "--output", "json"])

    payload = json.loads(result.stdout)  # parses, so nothing else is on stdout
    assert result.exit_code == 2
    assert payload["schema_version"] == 1
    assert payload["dry_run"] is True
    assert payload["has_changes"] is True
    assert payload["tables"][0]["name"] == "dev.silver.orders"


def test_plan_show_sql_appends_the_planned_statements(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("plan_show_sql", ORDERS_ONLY)

    result = runner.invoke(app, ["plan", module, "--show-sql"])

    assert result.exit_code == 2
    assert "PLANNED SQL" in result.stdout
    assert "-- dev.silver.orders" in result.stdout


def test_plan_reports_missing_connection_settings_without_a_traceback(
    runner, fake_engine, write_module, monkeypatch
):
    for name in ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    module = write_module("plan_no_env", ORDERS_ONLY)

    result = runner.invoke(app, ["plan", module])

    assert result.exit_code == 1
    assert "missing connection settings" in result.stderr
    assert "Traceback" not in result.stderr


def test_plan_reports_a_missing_module_as_a_config_error(runner, fake_engine, databricks_env):
    result = runner.invoke(app, ["plan", "does.not.exist"])

    assert result.exit_code == 1
    assert "does.not.exist" in result.stderr
    assert "Traceback" not in result.stderr


def test_version_prints_the_package_version(runner):
    import delta_engine

    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert delta_engine.__version__ in result.stdout
