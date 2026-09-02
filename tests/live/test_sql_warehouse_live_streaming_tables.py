"""
Live pins for the streaming-table facts the annotations scope is built on.

A streaming table's definition — schema, properties, and keys — is owned by
its pipeline. The line the platform draws is the defining SQL: comments and
Unity Catalog tags are alterable from outside the pipeline via the documented
ALTER STREAMING TABLE dialect and COMMENT ON, while schema, properties, and
constraints belong to CREATE OR REFRESH. The single test here states the
platform facts the engine's reader gate, validation gate, and SQL dialect
dispatch assume, and drives each one through the engine rather than through
hand-written SQL.

Two pins are deliberately absent. A raw ``DESCRIBE ... AS JSON`` pin proved
``type="STREAMING_TABLE"``, ``provider="delta"`` on every Live run and was
retired (2026-07-17): the end-to-end test carries the fact now — if the
platform changed either value, the reader's admit gate would fail that read.
And a rejection pin for plain ``ALTER TABLE ... SET TAGS`` never shipped:
the platform was observed tolerating it on a streaming table (2026-07-16);
the engine emits the documented ALTER STREAMING TABLE dialect and relies on
nothing being rejected.

Provisioning is quota-bound: the workspace tier allows one active DBSQL
pipeline at a time, so this module carries the ``streaming_table`` xdist group
(serialized onto one worker by the Live workflow's ``--dist loadgroup``) and
every pin shares one provisioned table.
"""

import pytest

pytest.importorskip("databricks.sql")


from delta_engine import SyncFailedError, TableRunStatus, ValidationFailure
from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.api.delta_table import ScopeName
from delta_engine.application.ports import TablePresent
from delta_engine.databricks import build_sql_engine
from delta_engine.domain.model import QualifiedName, TableKind
from delta_engine.schema import Column, DeltaTable, Integer
from tests.live.capabilities import require_databricks_capability
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
    read_live_table,
)

pytestmark = pytest.mark.xdist_group("streaming_table")

_QUOTA_ERROR_CONDITION = "QUOTA_EXCEEDED_EXCEPTION"
_QUOTA_RETRY_TIMEOUT_SECONDS = 100
_QUOTA_RETRY_WAIT_SECONDS = 20
_STREAMING_TABLE_UNAVAILABLE_CONDITIONS = {
    "FEATURE_NOT_ENABLED",
    "FEATURE_NOT_ON_CLASSIC_WAREHOUSE",
    "FEATURE_UNAVAILABLE",
    "STREAMING_TABLE_NOT_SUPPORTED",
}


def _report_quota_retry(condition: str, delay: float) -> None:
    print(
        f"streaming-table capability probe hit {condition}; retrying in {delay:g}s",
        flush=True,
    )


def _create_streaming_table(live_connection, live_tables) -> str:
    """Create a keyed streaming table over a Delta source; skip if the workspace cannot."""
    source_name = live_tables("st_source")
    table_name = live_tables("st")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(source_name)} (id INT NOT NULL) USING DELTA",
    )
    # The key is declared in the defining SQL, which is what makes this table a
    # realistic pipeline-owned fixture: the engine can never manage that key, so
    # a declaration must mirror it. NOT NULL is required twice over — Unity
    # Catalog will not key a nullable column, and the engine will not declare one.
    create_statement = (
        f"CREATE STREAMING TABLE {qualified_table(table_name)} "
        f"(id INT NOT NULL, CONSTRAINT {table_name}_pk PRIMARY KEY (id)) "
        f"AS SELECT id FROM STREAM({qualified_table(source_name)})"
    )
    # The one-pipeline quota releases asynchronously after a previous DROP TABLE,
    # so retry that condition against a deadline. The live_tables teardown owns
    # cleanup for both the streaming table and its source.
    require_databricks_capability(
        lambda: execute_sql(live_connection, create_statement),
        capability="streaming tables with key constraints",
        unavailable_conditions=_STREAMING_TABLE_UNAVAILABLE_CONDITIONS,
        retry_conditions={_QUOTA_ERROR_CONDITION},
        retry_timeout_seconds=_QUOTA_RETRY_TIMEOUT_SECONDS,
        retry_interval_seconds=_QUOTA_RETRY_WAIT_SECONDS,
        report_retry=_report_quota_retry,
    )
    return table_name


def test_the_engine_manages_a_streaming_tables_annotations_and_nothing_wider(
    live_connection, live_tables
):
    """The engine sets, converges, and clears a streaming table's annotations; wider scopes fail."""
    # Every pin below shares one provisioned table: the reported key, the read,
    # the round-trip, the convergence resync, the wider-scope refusal, and the
    # clear (see the module docstring on the pipeline quota).

    # Given a pipeline-owned streaming table carrying a key and a seeded tag
    table_name = _create_streaming_table(live_connection, live_tables)
    execute_sql(
        live_connection,
        f"ALTER STREAMING TABLE {qualified_table(table_name)} SET TAGS ('old'='remove-me')",
    )

    # Then the key the defining SQL declared is reported, not merely accepted. This
    # corrects the 2026-07-16 assumption that streaming tables return no
    # constraints, and it is what makes mirroring the right advice: were the key
    # unreported, a declaration that mirrored it would emit SetPrimaryKey and
    # fail as unmanaged drift instead of matching.
    assert read_live_table(live_connection, table_name)["primary_key"] == ("id",)

    # Then the kind is discovered, never declared: the engine reads a streaming
    # table as state to diff against rather than failing the read.
    reader = WarehouseReader(WarehouseSqlRunner(live_connection))
    state = reader.fetch_state(QualifiedName(live_catalog(), live_schema(), table_name))
    assert isinstance(state, TablePresent), state
    assert state.table.kind is TableKind.STREAMING_TABLE

    # When syncing an annotations-scoped declaration that mirrors the key
    # and annotates the table and column
    engine = build_sql_engine(live_connection)
    annotated_column = Column(
        "id", Integer(), nullable=False, comment="the id", tags={"pii": "low"}
    )
    table_comment = "Click events, owned by the ingest pipeline."
    table_tags = {"owner": "governance"}
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(annotated_column,),
        primary_key=["id"],  # mirrors the pipeline's key; never applied
        comment=table_comment,
        tags=table_tags,
        scope="annotations",
    )
    engine.sync(declaration)

    # Then every statement form the engine compiles against a streaming table
    # takes effect: SET TAGS at both levels, UNSET TAGS for the seeded tag this
    # declaration drops, the ALTER STREAMING TABLE column-comment clause, and
    # kind-independent COMMENT ON.
    annotated = read_live_table(live_connection, table_name)
    assert annotated["comment"] == table_comment
    assert [column["comment"] for column in annotated["columns"]] == ["the id"]
    assert annotated["table_tags"] == table_tags
    assert annotated["column_tags"] == {("id", "pii"): "low"}

    # Then the reader round-trips everything the executor just wrote: a resync
    # finds nothing left to do. This is also what verifies the mirroring
    # contract — had the platform not reported the pipeline's key, mirroring it
    # would have emitted SetPrimaryKey and failed UnmanagedAspectDrift instead
    # of converging.
    assert engine.sync(declaration).has_changes is False

    # Then anything wider than annotations is refused before planning — even
    # when the declaration mirrors the observed state exactly, so the refusal
    # is about the table's kind, not about drift.
    wider_scopes: tuple[ScopeName, ...] = ("metadata", "full")
    for scope in wider_scopes:
        wider_declaration = DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(annotated_column,),
            primary_key=["id"],
            comment=table_comment,
            tags=table_tags,
            scope=scope,
        )
        with pytest.raises(SyncFailedError) as error:
            engine.sync(wider_declaration)

        [table_report] = error.value.report.table_runs
        assert table_report.status is TableRunStatus.PLANNING_FAILED
        assert "StreamingTableAnnotationsOnly" in {
            failure.rule_name
            for failure in table_report.failures
            if isinstance(failure, ValidationFailure)
        }
        assert table_report.compiled is None
        refused = read_live_table(live_connection, table_name)
        assert refused["comment"] == table_comment
        assert [column["comment"] for column in refused["columns"]] == ["the id"]
        assert refused["table_tags"] == table_tags
        assert refused["column_tags"] == {("id", "pii"): "low"}
        assert refused["primary_key"] == ("id",)

    # When syncing a declaration that clears every annotation — the other
    # half of managing an aspect, and the half with a platform quirk: an
    # empty desired comment compiles to COMMENT '' rather than UNSET
    # COMMENT, which SQL warehouses reject
    cleared = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer(), nullable=False),),
        primary_key=["id"],
        scope="annotations",
    )
    engine.sync(cleared)

    # Then every annotation is emptied and the resync converges, proving ''
    # comes back as the empty comment the reader observes
    emptied = read_live_table(live_connection, table_name)
    assert emptied["comment"] == ""
    assert [column["comment"] for column in emptied["columns"]] == [""]
    assert emptied["table_tags"] == {}
    assert emptied["column_tags"] == {}
    assert engine.sync(cleared).has_changes is False
