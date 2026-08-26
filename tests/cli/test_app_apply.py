"""Behaviour of the executing ``delta-engine apply`` workflow."""

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


def test_execution_failure_prints_the_report_and_exits_one(
    runner, failing_engine, databricks_env, write_module
):
    module = write_module("apply_execution_failure", ORDERS_ONLY)

    result = runner.invoke(app, ["apply", f"{module}:all_tables"])

    assert result.exit_code == 1
    assert "EXECUTION_FAILED" in result.stdout
    assert "PermissionDenied" in result.stdout
    assert "not applied" in result.stdout
    assert "SQL: -- dev.silver.orders: CreateTable" in result.stdout


def test_validation_failure_executes_nothing_and_exits_one(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_invalid", NOT_NULL_DRIFT_ORDERS)
    fake_engine.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["apply", f"{module}:all_tables"])

    assert result.exit_code == 1
    assert "PLANNING_FAILED" in result.stdout
    assert "EXECUTED SQL" not in result.stdout
