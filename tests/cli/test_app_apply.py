"""Behaviour of `delta-engine apply`: execution, failure rendering, user-bug tracebacks."""

import json

from delta_engine.application.ports import TablePresent
from delta_engine.cli.app import app
from delta_engine.domain.model import Column, ObservedTable, QualifiedName, String
from tests.cli.conftest import ORDERS_ONLY

RAISES_ON_IMPORT = """
    raise RuntimeError("boom in user code")
"""

DUPLICATE_ORDERS = """
    from delta_engine.schema import Column, DeltaTable, String

    orders_a = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
    orders_b = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))
"""

INVALID_DRIFT = """
    from delta_engine.schema import Column, DeltaTable, String

    orders = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(Column("id", String()), Column("amount", String(), nullable=False)),
    )
"""


def _observed_orders() -> TablePresent:
    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName("dev", "silver", "orders"),
            columns=(Column("id", String()),),
        )
    )


def test_apply_executes_pending_changes_and_exits_zero(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_creates", ORDERS_ONLY)

    result = runner.invoke(app, ["apply", module])

    assert result.exit_code == 0
    assert "SYNC REPORT" in result.stdout
    assert "DRY RUN" not in result.stdout
    assert "1/1" in result.stdout  # applied/planned statement count in the grid


def test_apply_is_a_no_op_when_already_in_sync(runner, fake_engine, databricks_env, write_module):
    module = write_module("apply_in_sync", ORDERS_ONLY)
    fake_engine.states["dev.silver.orders"] = _observed_orders()

    result = runner.invoke(app, ["apply", module])

    assert result.exit_code == 0
    assert "no changes" in result.stdout


def test_apply_renders_the_failure_report_and_exits_one(
    runner, fake_engine, databricks_env, write_module
):
    # Given a declaration whose drift fails validation (NOT NULL add on existing table)
    module = write_module("apply_invalid", INVALID_DRIFT)
    fake_engine.states["dev.silver.orders"] = _observed_orders()

    result = runner.invoke(app, ["apply", module])

    assert result.exit_code == 1
    assert "Failures" in result.stdout


def test_apply_failure_in_json_mode_emits_the_report_payload(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_invalid_json", INVALID_DRIFT)
    fake_engine.states["dev.silver.orders"] = _observed_orders()

    result = runner.invoke(app, ["apply", module, "--output", "json"])

    payload = json.loads(result.stdout)
    assert result.exit_code == 1
    assert payload["has_failures"] is True
    assert payload["tables"][0]["status"] == "VALIDATION_FAILED"


def test_a_module_that_raises_shows_the_users_traceback(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_user_bug", RAISES_ON_IMPORT)

    result = runner.invoke(app, ["apply", module])

    assert result.exit_code == 1
    assert "boom in user code" in result.stderr
    assert "RuntimeError" in result.stderr


def test_duplicate_qualified_names_are_a_config_error(
    runner, fake_engine, databricks_env, write_module
):
    module = write_module("apply_duplicates", DUPLICATE_ORDERS)

    result = runner.invoke(app, ["apply", module])

    assert result.exit_code == 1
    assert "orders" in result.stderr
    assert "Traceback" not in result.stderr
