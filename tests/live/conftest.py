"""
Fixtures for credentialed live tests against a real Databricks SQL warehouse.

Every test collected from this directory is automatically marked
``databricks_e2e`` — directory membership alone defines the live suite, so a
new file cannot leak into default runs by forgetting a marker line. The
addopts marker filter excludes the suite from every default pytest run; it
only runs when requested explicitly (a manual run or the live CI job):

    databricks auth login --host https://<workspace>   # once (OAuth), or set
                                                       # DATABRICKS_CONFIG_PROFILE
    export DATABRICKS_HTTP_PATH=...
    export DELTA_ENGINE_E2E_CATALOG=... DELTA_ENGINE_E2E_SCHEMA=...
    uv run pytest tests/live -m databricks_e2e --no-cov

Every test allocates uniquely named tables through the ``live_tables`` factory
and drops them afterwards, so runs are safe to repeat against a shared schema.
"""

from collections.abc import Callable
import os
from pathlib import Path
from uuid import uuid4

import pytest

_LIVE_REQUIRED_ENV = ("DELTA_ENGINE_E2E_CATALOG", "DELTA_ENGINE_E2E_SCHEMA")
_LIVE_DIRECTORY = Path(__file__).parent

type LiveTableFactory = Callable[[str], str]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark every test in this directory as credentialed-live."""
    for item in items:
        if _LIVE_DIRECTORY in item.path.parents:
            item.add_marker(pytest.mark.databricks_e2e)


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
