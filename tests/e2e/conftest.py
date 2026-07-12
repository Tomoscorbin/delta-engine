from collections.abc import Callable
import os
from uuid import uuid4

import pytest

from delta_engine.adapters.databricks.spark.reader import SparkReader

_LIVE_REQUIRED_ENV = ("DELTA_ENGINE_E2E_CATALOG", "DELTA_ENGINE_E2E_SCHEMA")

type LiveTableFactory = Callable[[str], str]


@pytest.fixture(autouse=True)
def local_spark_databricks_reader_compat(monkeypatch):
    def _table_exists(self, qualified_name):
        # Local Spark fallback for existence checks.
        return self.spark.catalog.tableExists(f"{qualified_name.schema}.{qualified_name.name}")

    monkeypatch.setattr(SparkReader, "_table_exists", _table_exists, raising=True)


@pytest.fixture(scope="module")
def live_connection():
    missing = [name for name in _LIVE_REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(f"warehouse e2e env vars not set: {', '.join(missing)}")

    from tests.e2e.databricks_connection import open_sql_warehouse_connection

    with open_sql_warehouse_connection() as connection:
        yield connection


@pytest.fixture
def live_tables(live_connection) -> LiveTableFactory:
    from tests.e2e.sql_warehouse_live_helpers import execute_sql, qualified_table

    names: list[str] = []

    def allocate(label: str) -> str:
        name = f"de_live_{label}_{uuid4().hex[:8]}"
        names.append(name)
        return name

    yield allocate

    for name in reversed(names):
        execute_sql(live_connection, f"DROP TABLE IF EXISTS {qualified_table(name)}")
