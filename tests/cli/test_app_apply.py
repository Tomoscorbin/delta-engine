"""Behaviour of the executing ``delta-engine apply`` workflow."""

from delta_engine.cli.app import app
from tests.cli.conftest import ORDERS_ONLY, observed_orders


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
