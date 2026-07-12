"""
Credentialed end-to-end tests for the warehouse backend.

Run manually against a real SQL warehouse (never in default CI):

    export DATABRICKS_SERVER_HOSTNAME=... DATABRICKS_HTTP_PATH=... DATABRICKS_TOKEN=...
    export DELTA_ENGINE_E2E_CATALOG=... DELTA_ENGINE_E2E_SCHEMA=...
    uv run pytest -m databricks_e2e tests/e2e/test_warehouse_e2e.py --no-cov

Besides the round trip, these tests pin the spec's verification risks:
information_schema sees a just-created table within one sync, and the
connector returns DESCRIBE DETAIL's map/array fields as JSON strings the
reader can parse.
"""

import os
from uuid import uuid4

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine  # noqa: E402
from delta_engine.schema import Column, DeltaTable, Integer, Property, String  # noqa: E402

pytestmark = pytest.mark.databricks_e2e

_REQUIRED_ENV = (
    "DATABRICKS_SERVER_HOSTNAME",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_TOKEN",
    "DELTA_ENGINE_E2E_CATALOG",
    "DELTA_ENGINE_E2E_SCHEMA",
)


@pytest.fixture(scope="module")
def connection():
    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(f"warehouse e2e env vars not set: {', '.join(missing)}")
    with databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as open_connection:
        yield open_connection


@pytest.fixture
def table_name(connection):
    name = f"warehouse_e2e_{uuid4().hex[:8]}"
    yield name
    catalog = os.environ["DELTA_ENGINE_E2E_CATALOG"]
    schema = os.environ["DELTA_ENGINE_E2E_SCHEMA"]
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{name}`")


def test_sync_creates_reads_back_and_is_idempotent(connection, table_name):
    # Given a declared table with a comment, clustering, a primary key, a
    # managed property, and a table tag (exercises the JSON detail fields,
    # primary_key_from_rows, table_tags_from_rows, and the properties
    # registry filter on the read back)
    table = DeltaTable(
        catalog=os.environ["DELTA_ENGINE_E2E_CATALOG"],
        schema=os.environ["DELTA_ENGINE_E2E_SCHEMA"],
        name=table_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("name", String(), comment="customer name"),
        ),
        comment="warehouse e2e table",
        clustered_by=("id",),
        primary_key=["id"],
        properties={Property.CHANGE_DATA_FEED: "true"},
        tags={"owner": "warehouse-e2e"},
    )
    engine = build_sql_engine(connection)

    # When syncing it twice
    first = engine.sync(table)
    second = engine.sync(table)

    # Then the first run creates it and the second finds nothing to do:
    # a clean second read proves information_schema saw the new table and
    # the JSON-string detail fields parsed correctly
    assert first.has_failures is False
    assert first.has_changes is True
    assert second.has_failures is False
    assert second.has_changes is False


def test_dry_run_previews_sql_without_executing(connection, table_name):
    # Given a declared table that does not exist yet
    table = DeltaTable(
        catalog=os.environ["DELTA_ENGINE_E2E_CATALOG"],
        schema=os.environ["DELTA_ENGINE_E2E_SCHEMA"],
        name=table_name,
        columns=(Column("id", Integer(), nullable=False),),
    )
    engine = build_sql_engine(connection)

    # When syncing as a dry run
    report = engine.sync(table, dry_run=True)

    # Then the preview holds the CREATE TABLE statement
    [table_report] = list(report)
    assert any(
        "CREATE TABLE" in statement.upper() for statement in table_report.planned_sql_statements
    )

    # And nothing was created: a second dry run still finds the same drift
    followup = engine.sync(table, dry_run=True)
    assert followup.has_changes is True
