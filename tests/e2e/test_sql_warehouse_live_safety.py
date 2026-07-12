"""Live proofs that unsafe, out-of-scope, or server-rejected DDL leaves Unity Catalog unchanged."""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine import (
    ExecutionFailure,
    SyncFailedError,
    TableRunStatus,
    ValidationFailure,
)
from delta_engine.databricks import build_sql_engine
from delta_engine.schema import (
    Column,
    Date,
    DeltaTable,
    ForeignKey,
    Integer,
    Long,
    Property,
    String,
    TimestampNtz,
)
from tests.e2e.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
    read_live_table,
    table_exists,
)

pytestmark = pytest.mark.databricks_e2e


def _assert_rejected_without_catalog_change(connection, table_name, declaration):
    before = read_live_table(connection, table_name)
    with pytest.raises(SyncFailedError):
        build_sql_engine(connection).sync(declaration)
    assert read_live_table(connection, table_name) == before


def test_non_nullable_column_add_is_rejected_without_catalog_change(live_connection, live_tables):
    table_name = live_tables("reject_required_add")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
        )
    )

    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()), Column("required", String(), nullable=False)),
        ),
    )


def test_nullability_tightening_is_rejected_without_catalog_change(live_connection, live_tables):
    table_name = live_tables("reject_not_null")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
        )
    )

    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer(), nullable=False),),
        ),
    )


def test_type_narrowing_is_rejected_without_catalog_change(live_connection, live_tables):
    table_name = live_tables("reject_narrow")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Long()),),
        )
    )

    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
        ),
    )


def test_column_drop_without_column_mapping_is_rejected_without_catalog_change(
    live_connection, live_tables
):
    table_name = live_tables("reject_drop")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()), Column("legacy", String())),
        )
    )

    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
        ),
    )


def test_partitioning_change_is_rejected_without_catalog_change(live_connection, live_tables):
    table_name = live_tables("reject_partition")
    columns = (Column("id", Integer()), Column("region", String()))
    engine = build_sql_engine(live_connection)
    engine.sync(DeltaTable(live_catalog(), live_schema(), table_name, columns=columns))

    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=columns,
            partitioned_by=("region",),
        ),
    )


def test_permanent_column_mapping_transition_is_rejected_without_catalog_change(
    live_connection, live_tables
):
    table_name = live_tables("reject_mapping")
    columns = (Column("id", Integer()),)
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=columns,
            properties={Property.COLUMN_MAPPING_MODE: "name"},
        )
    )

    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=columns,
            properties={Property.COLUMN_MAPPING_MODE: "none"},
        ),
    )


def test_referenced_primary_key_change_is_rejected_without_catalog_change(
    live_connection, live_tables
):
    parent_name = live_tables("referenced_parent")
    child_name = live_tables("referencing_child")
    parent_columns = (Column("id", Integer(), nullable=False),)
    parent = DeltaTable(
        live_catalog(),
        live_schema(),
        parent_name,
        columns=parent_columns,
        primary_key=("id",),
    )
    child = DeltaTable(
        live_catalog(),
        live_schema(),
        child_name,
        columns=(Column("id", Integer()), Column("parent_id", Integer())),
        foreign_keys=(ForeignKey(columns={"parent_id": "id"}, references=parent),),
    )
    build_sql_engine(live_connection).sync(child, parent)
    parent_before = read_live_table(live_connection, parent_name)
    child_before = read_live_table(live_connection, child_name)

    with pytest.raises(SyncFailedError) as error:
        build_sql_engine(live_connection).sync(
            DeltaTable(live_catalog(), live_schema(), parent_name, columns=parent_columns)
        )

    # The child is not part of this sync, yet the engine still observes its
    # foreign key through the catalog and refuses client-side: the report
    # names the referencing constraint and no DDL was even planned.
    [parent_report] = [
        table_report
        for table_report in error.value.report.table_reports
        if table_report.qualified_name.name == parent_name
    ]
    assert parent_report.status is TableRunStatus.VALIDATION_FAILED
    [failure] = parent_report.failures
    assert isinstance(failure, ValidationFailure)
    assert f"{child_name}_parent_id_fk" in failure.message
    assert parent_report.planned_sql_statements == ()
    assert read_live_table(live_connection, parent_name) == parent_before
    assert read_live_table(live_connection, child_name) == child_before


def test_restricted_scope_drift_is_rejected_without_catalog_change(live_connection, live_tables):
    table_name = live_tables("reject_scope_drift")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (id BIGINT) USING DELTA",
    )

    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
            tags={"should_not": "appear"},
            scope="metadata",
        ),
    )


def test_restricted_scope_does_not_create_missing_table(live_connection, live_tables):
    table_name = live_tables("missing_scope")
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer()),),
        tags={"owner": "governance"},
        scope="tags",
    )

    with pytest.raises(SyncFailedError):
        build_sql_engine(live_connection).sync(declaration)

    assert table_exists(live_connection, table_name) is False


def test_independent_table_still_changes_when_another_table_is_rejected(
    live_connection, live_tables
):
    unsafe_name = live_tables("batch_unsafe")
    healthy_name = live_tables("batch_healthy")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            unsafe_name,
            columns=(Column("id", Integer()),),
        )
    )
    unsafe_before = read_live_table(live_connection, unsafe_name)
    unsafe = DeltaTable(
        live_catalog(),
        live_schema(),
        unsafe_name,
        columns=(Column("id", Integer()), Column("required", String(), nullable=False)),
    )
    healthy = DeltaTable(
        live_catalog(),
        live_schema(),
        healthy_name,
        columns=(Column("id", Integer()), Column("name", String())),
        comment="independent table still applied",
    )

    with pytest.raises(SyncFailedError):
        engine.sync(unsafe, healthy)

    assert read_live_table(live_connection, unsafe_name) == unsafe_before
    healthy_state = read_live_table(live_connection, healthy_name)
    assert [column["column_name"] for column in healthy_state["columns"]] == ["id", "name"]
    assert healthy_state["comment"] == "independent table still applied"


def test_server_rejected_statement_surfaces_as_typed_execution_failure(
    live_connection, live_tables
):
    table_name = live_tables("reject_execution")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("event_date", Date()),),
            properties={Property.TYPE_WIDENING: "true"},
        )
    )
    before = read_live_table(live_connection, table_name)

    # Widening date -> timestamp_ntz passes client-side validation, but the
    # live table lacks the timestampNtz table feature, so the warehouse
    # rejects the ALTER. The report must carry that server rejection as a
    # typed execution failure holding the failing statement.
    with pytest.raises(SyncFailedError) as error:
        engine.sync(
            DeltaTable(
                live_catalog(),
                live_schema(),
                table_name,
                columns=(Column("event_date", TimestampNtz()),),
                properties={Property.TYPE_WIDENING: "true"},
            )
        )

    [table_report] = error.value.report.table_reports
    assert table_report.status is TableRunStatus.EXECUTION_FAILED
    [failure] = table_report.failures
    assert isinstance(failure, ExecutionFailure)
    assert "ALTER COLUMN" in failure.statement
    assert failure.message
    assert read_live_table(live_connection, table_name) == before
