"""Behaviour of the executing ``delta-engine apply`` workflow."""

import json

from delta_engine.application.errors import ReadError
from delta_engine.cli.app import app
from tests.cli.conftest import NOT_NULL_DRIFT_ORDERS, ORDERS_ONLY, observed_orders


def test_in_sync_apply_exits_zero_and_reports_unchanged(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_in_sync", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["apply", f"{module}:all_tables"])

    assert result.exit_code == 0
    assert "TARGET" in result.stdout
    assert "DIFF" in result.stdout
    assert "SYNC REPORT" in result.stdout
    assert "unchanged" in result.stdout
    assert "PLAN — no planned SQL executed" not in result.stdout


def test_changed_apply_exits_zero_and_prints_the_executed_sql(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_drift", ORDERS_ONLY)

    result = runner.invoke(app, ["apply", f"{module}:all_tables"])

    assert result.exit_code == 0
    assert "applied" in result.stdout
    assert "EXECUTED SQL" in result.stdout
    assert "-- dev.silver.orders: CreateTable" in result.stdout
    assert "PLANNED SQL" not in result.stdout
    assert "PLAN — no planned SQL executed" not in result.stdout


def test_json_output_reports_the_executed_run(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration absent from the live catalog
    module = write_module("apply_json_drift", ORDERS_ONLY)

    # When applying with JSON output
    result = runner.invoke(app, ["apply", f"{module}:all_tables", "--output", "json"])

    # Then stdout is one run report recording the real execution
    assert result.exit_code == 0
    report = json.loads(result.stdout)
    assert report["schema_version"] == 2
    assert report["dry_run"] is False
    assert report["tables"][0]["planned_sql_statements"] == [
        "-- dev.silver.orders: CreateTable"
    ]
    assert report["tables"][0]["execution"] == {"applied": 1, "total": 1}


def test_execution_failure_prints_the_report_and_exits_one(
    runner, failing_engine, databricks_env, write_module
):
    # Given a declared table whose statements fail at the warehouse
    module = write_module("apply_execution_failure", ORDERS_ONLY)

    # When applying the declaration
    result = runner.invoke(app, ["apply", f"{module}:all_tables"])

    # Then the report names the failed statement and the command exits one
    assert result.exit_code == 1
    assert "EXECUTION_FAILED" in result.stdout
    assert "PermissionDenied" in result.stdout
    assert "not applied" in result.stdout
    assert "EXECUTED SQL" in result.stdout
    assert "SQL: -- dev.silver.orders: CreateTable" in result.stdout


def test_read_failure_prints_the_report_and_exits_one(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declared table whose catalog state cannot be read
    module = write_module("apply_read_failure", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = ReadError("PermissionDenied", "cannot inspect table")

    # When applying the declaration
    result = runner.invoke(app, ["apply", f"{module}:all_tables"])

    # Then the report shows the read failure, nothing executes, and the command exits one
    assert result.exit_code == 1
    assert "READ_FAILED" in result.stdout
    assert "PermissionDenied" in result.stdout
    assert "EXECUTED SQL" not in result.stdout


def test_validation_failure_executes_nothing_and_exits_one(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_invalid", NOT_NULL_DRIFT_ORDERS)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["apply", f"{module}:all_tables"])

    assert result.exit_code == 1
    assert "PLANNING_FAILED" in result.stdout
    assert "EXECUTED SQL" not in result.stdout
