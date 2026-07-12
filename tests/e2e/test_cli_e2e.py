"""
Credentialed CLI smoke test.

Run manually against a real SQL warehouse (never in default CI):

    export DATABRICKS_HOST=... DATABRICKS_HTTP_PATH=...
    # Configure any Databricks unified-auth method (profile, PAT, OAuth, or OIDC).
    export DELTA_ENGINE_E2E_CATALOG=... DELTA_ENGINE_E2E_SCHEMA=...
    uv run pytest -m databricks_e2e tests/e2e/test_cli_e2e.py --no-cov

`plan` is a dry run, so nothing is created and no cleanup is needed.
"""

import json
import os
import sys
from textwrap import dedent
from uuid import uuid4

import pytest

pytest.importorskip("typer")
pytest.importorskip("databricks.sdk")
pytest.importorskip("databricks.sql")

from typer.testing import CliRunner

from delta_engine.cli.app import app

pytestmark = pytest.mark.databricks_e2e

_REQUIRED_ENV = (
    "DATABRICKS_HOST",
    "DATABRICKS_HTTP_PATH",
    "DELTA_ENGINE_E2E_CATALOG",
    "DELTA_ENGINE_E2E_SCHEMA",
)


def test_plan_reports_pending_create_for_a_missing_table(tmp_path, monkeypatch):
    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(f"cli e2e env vars not set: {', '.join(missing)}")

    # Given a declaration for a table that does not exist in the catalog
    table_name = f"cli_e2e_{uuid4().hex[:8]}"
    (tmp_path / "cli_e2e_tables.py").write_text(
        dedent(
            f"""
            import os

            from delta_engine.schema import Column, DeltaTable, Integer

            table = DeltaTable(
                os.environ["DELTA_ENGINE_E2E_CATALOG"],
                os.environ["DELTA_ENGINE_E2E_SCHEMA"],
                "{table_name}",
                columns=(Column("id", Integer(), nullable=False),),
            )
            """
        )
    )
    monkeypatch.chdir(tmp_path)

    try:
        # When running the CLI gate in JSON mode
        result = CliRunner().invoke(
            app,
            ["plan", "cli_e2e_tables:table", "--output", "json"],
        )

        # Then the valid pending CREATE is reported and succeeds by default
        payload = json.loads(result.stdout)
        assert result.exit_code == 0
        assert payload["has_changes"] is True
        assert payload["has_failures"] is False
        statements = payload["tables"][0]["planned_sql_statements"]
        assert any("CREATE TABLE" in statement.upper() for statement in statements)
    finally:
        # Match the clean-import guarantee the tests/cli fixtures provide:
        # drop the cached module and the cwd entry the loader prepended.
        sys.modules.pop("cli_e2e_tables", None)
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))
