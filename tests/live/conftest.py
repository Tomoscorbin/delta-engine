"""
Fixtures for credentialed live tests against a real Databricks SQL warehouse.

These tests are excluded from the default pytest run by the addopts marker
filter and only run when requested explicitly (a manual run or the live CI
job):

    export DATABRICKS_SERVER_HOSTNAME=... DATABRICKS_HTTP_PATH=...
    export DATABRICKS_TOKEN=...            # or profile/OIDC, see databricks_connection.py
    export DELTA_ENGINE_E2E_CATALOG=... DELTA_ENGINE_E2E_SCHEMA=...
    uv run pytest tests/live -m databricks_e2e --no-cov

Every test allocates uniquely named tables through the ``live_tables`` factory
and drops them afterwards, so runs are safe to repeat against a shared schema.
"""

from collections.abc import Callable
import os
from uuid import uuid4

import pytest

_LIVE_REQUIRED_ENV = ("DELTA_ENGINE_E2E_CATALOG", "DELTA_ENGINE_E2E_SCHEMA")

type LiveTableFactory = Callable[[str], str]


@pytest.fixture(scope="module")
def live_connection():
    missing = [name for name in _LIVE_REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(f"warehouse e2e env vars not set: {', '.join(missing)}")

    from tests.live.databricks_connection import open_sql_warehouse_connection

    with open_sql_warehouse_connection() as connection:
        yield connection


@pytest.fixture
def live_tables(live_connection) -> LiveTableFactory:
    from tests.live.sql_warehouse_live_helpers import execute_sql, qualified_table

    names: list[str] = []

    def allocate(label: str) -> str:
        name = f"de_live_{label}_{uuid4().hex[:8]}"
        names.append(name)
        return name

    yield allocate

    for name in reversed(names):
        execute_sql(live_connection, f"DROP TABLE IF EXISTS {qualified_table(name)}")
