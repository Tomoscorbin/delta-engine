"""Live proofs that unsafe, out-of-scope, or server-rejected DDL leaves Unity Catalog unchanged."""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine import (
    ExecutionFailure,
    ForeignKeyFailure,
    ReadFailure,
    SyncFailedError,
    TableRunStatus,
    ValidationFailure,
    render_report,
)
from delta_engine.databricks import build_sql_engine
from delta_engine.schema import (
    Column,
    DeltaTable,
    ForeignKey,
    Integer,
    Long,
    String,
    TableProperty,
)
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
    read_live_table,
    table_exists,
)


def _assert_rejected_without_catalog_change(connection, table_name, declaration):
    before = read_live_table(connection, table_name)
    with pytest.raises(SyncFailedError):
        build_sql_engine(connection).sync(declaration)
    assert read_live_table(connection, table_name) == before


def test_non_nullable_column_add_is_rejected_without_catalog_change(live_connection, live_tables):
    """Adding a non-nullable column is rejected, and the catalog is left unchanged."""
    # Given a synced single-column table
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

    # Then a declaration adding a non-nullable column is rejected without
    # catalog change
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
    """Tightening a column to non-nullable is rejected, and the catalog is unchanged."""
    # Given a synced table with a nullable column
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

    # Then a declaration tightening it to non-nullable is rejected without
    # catalog change
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
    """Narrowing a column's type is rejected, and the catalog is left unchanged."""
    # Given a synced table with a Long column
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

    # Then a declaration narrowing it to Integer is rejected without
    # catalog change
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
    """Dropping a column without column mapping is rejected; the catalog is unchanged."""
    # Given a synced two-column table without column mapping
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

    # Then a declaration dropping a column is rejected without catalog change
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
    """Changing an existing table's partitioning is rejected; the catalog is unchanged."""
    # Given a synced unpartitioned table
    table_name = live_tables("reject_partition")
    columns = (Column("id", Integer()), Column("region", String()))
    engine = build_sql_engine(live_connection)
    engine.sync(DeltaTable(live_catalog(), live_schema(), table_name, columns=columns))

    # Then a declaration introducing partitioning is rejected without
    # catalog change
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


def test_partitioned_to_clustered_conversion_is_rejected_without_catalog_change(
    live_connection, live_tables
):
    """Converting a partitioned table to clustering is rejected, and nothing changes."""
    # Given a partitioned live table
    table_name = live_tables("reject_convert")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} "
        "(id INT, region STRING) USING DELTA PARTITIONED BY (region)",
    )

    # Then a clustered declaration for it is rejected without catalog change
    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()), Column("region", String())),
            clustered_by=("id",),
        ),
    )


def test_permanent_column_mapping_transition_is_rejected_without_catalog_change(
    live_connection, live_tables
):
    """Turning column mapping off is rejected, and the catalog is left unchanged."""
    # Given a synced table with column mapping enabled
    table_name = live_tables("reject_mapping")
    columns = (Column("id", Integer()),)
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=columns,
            properties={TableProperty.COLUMN_MAPPING_MODE: "name"},
        )
    )

    # Then a declaration turning the mapping off is rejected without
    # catalog change
    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=columns,
            properties={TableProperty.COLUMN_MAPPING_MODE: "none"},
        ),
    )


def test_referenced_primary_key_change_is_rejected_without_catalog_change(
    live_connection, live_tables
):
    """Changing a primary key referenced by another table's FK is rejected client-side."""
    # Given a synced parent whose primary key a child's foreign key references
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

    # When syncing the parent alone with its key removed
    with pytest.raises(SyncFailedError) as error:
        build_sql_engine(live_connection).sync(
            DeltaTable(live_catalog(), live_schema(), parent_name, columns=parent_columns)
        )

    # Then the engine refuses client-side: the child is not part of this sync,
    # yet its foreign key is observed through the catalog — the report names
    # the referencing constraint, no DDL was even planned, and neither table
    # changed
    [parent_report] = [
        table_report
        for table_report in error.value.report.table_runs
        if table_report.qualified_name.name == parent_name
    ]
    assert parent_report.status is TableRunStatus.PLANNING_FAILED
    [failure] = parent_report.failures
    assert isinstance(failure, ValidationFailure)
    [(observed_constraint_name, *_)] = child_before["foreign_keys"]
    assert observed_constraint_name in failure.message
    assert parent_report.compiled is None
    assert read_live_table(live_connection, parent_name) == parent_before
    assert read_live_table(live_connection, child_name) == child_before


def test_restricted_scope_drift_is_rejected_without_catalog_change(live_connection, live_tables):
    """Drift outside a restricted scope is rejected, and the catalog is left unchanged."""
    # Given a live BIGINT column outside the declaration's metadata scope
    table_name = live_tables("reject_scope_drift")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (id BIGINT) USING DELTA",
    )

    # Then a metadata-scoped declaration typing the column as Integer is
    # rejected without catalog change
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
    """A restricted-scope declaration defers a missing table instead of creating it."""
    # Given a tags-scoped declaration for a table absent from the catalog
    table_name = live_tables("missing_scope")
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer()),),
        tags={"owner": "governance"},
        scope="tags",
    )

    # When syncing it
    report = build_sql_engine(live_connection).sync(declaration)

    # Then the table is deferred without failure and was not created
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.DEFERRED
    assert table_report.has_failures is False
    assert table_exists(live_connection, table_name) is False


def test_independent_table_still_changes_when_another_table_is_rejected(
    live_connection, live_tables
):
    """An independent table still syncs when another table in the run is rejected."""
    # Given one declaration carrying an unsafe change and one healthy,
    # independent declaration
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

    # When syncing both in one run
    with pytest.raises(SyncFailedError):
        engine.sync(unsafe, healthy)

    # Then the unsafe table is untouched and the healthy one was applied
    assert read_live_table(live_connection, unsafe_name) == unsafe_before
    healthy_state = read_live_table(live_connection, healthy_name)
    assert [column["column_name"] for column in healthy_state["columns"]] == ["id", "name"]
    assert healthy_state["comment"] == "independent table still applied"


def test_server_rejected_statement_surfaces_as_typed_execution_failure(
    live_connection, live_tables
):
    """A warehouse-rejected statement surfaces as a typed execution failure with its SQL."""
    # Given an existing table whose catalog state will be compared after failure
    table_name = live_tables("reject_execution")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
        )
    )
    before = read_live_table(live_connection, table_name)

    # A 300-character column name passes client-side validation (column names
    # are exempt from the object-name rule; their special characters are
    # governed by column mapping instead) but Unity Catalog caps identifiers
    # at 255 characters, so ADD COLUMN is rejected server-side. Any
    # client-admitted, warehouse-rejected statement works as the vehicle here;
    # the previous one, a dotted tag key, is now rejected at declaration.
    # When syncing a declaration that produces the server-rejected statement
    with pytest.raises(SyncFailedError) as error:
        engine.sync(
            DeltaTable(
                live_catalog(),
                live_schema(),
                table_name,
                columns=(Column("id", Integer()), Column("c" * 300, Integer())),
            )
        )

    # Then the failure is typed, carries its SQL, and leaves catalog state unchanged
    [table_report] = error.value.report.table_runs
    assert table_report.status is TableRunStatus.EXECUTION_FAILED
    [failure] = table_report.failures
    assert isinstance(failure, ExecutionFailure)
    assert "ADD COLUMN" in failure.statement
    assert failure.message
    assert read_live_table(live_connection, table_name) == before


def test_widening_without_the_type_widening_property_is_rejected_without_catalog_change(
    live_connection, live_tables
):
    """Widening without the type-widening property is rejected, and nothing changes."""
    # Given a synced Integer column without the type-widening property
    table_name = live_tables("reject_widen")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()),),
        )
    )

    # Then a declaration widening it to Long is rejected without catalog change
    _assert_rejected_without_catalog_change(
        live_connection,
        table_name,
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Long()),),
        ),
    )


def test_undeclared_managed_property_is_rejected_without_catalog_change(
    live_connection, live_tables
):
    """A managed property set outside the declaration is rejected, not reconciled."""
    # Exact-declaration semantics: a managed key set outside the declaration
    # is drift the sync must refuse to reconcile silently, not quietly unset.

    # Given a synced table with a managed property set behind the engine's back
    table_name = live_tables("reject_undeclared")
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer()),),
    )
    engine = build_sql_engine(live_connection)
    engine.sync(declaration)
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(table_name)} "
        "SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 9 days')",
    )

    # Then resyncing the original declaration is rejected without catalog change
    _assert_rejected_without_catalog_change(live_connection, table_name, declaration)


def test_child_with_foreign_key_is_blocked_when_its_parent_is_rejected(
    live_connection, live_tables
):
    """A child table is blocked when its foreign-key parent is rejected in the same run."""
    # Given a new child whose foreign key references a parent carrying an
    # unsafe change
    parent_name = live_tables("blocked_parent")
    child_name = live_tables("blocked_child")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            parent_name,
            columns=(Column("id", Integer(), nullable=False),),
            primary_key=("id",),
        )
    )
    parent_before = read_live_table(live_connection, parent_name)
    unsafe_parent = DeltaTable(
        live_catalog(),
        live_schema(),
        parent_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("required", String(), nullable=False),
        ),
        primary_key=("id",),
    )
    child = DeltaTable(
        live_catalog(),
        live_schema(),
        child_name,
        columns=(Column("id", Integer(), nullable=False), Column("parent_id", Integer())),
        foreign_keys=(ForeignKey(columns={"parent_id": "id"}, references=unsafe_parent),),
    )

    # When syncing both in one run
    with pytest.raises(SyncFailedError) as error:
        engine.sync(child, unsafe_parent)

    # Then the child is blocked: it was individually safe to create, but its
    # foreign key depends on a table whose own change was rejected —
    # dependency resolution must not create it against an unreconciled parent
    [child_report] = [
        table_report
        for table_report in error.value.report.table_runs
        if table_report.qualified_name.name == child_name
    ]
    assert child_report.status is TableRunStatus.FOREIGN_KEY_FAILED
    [failure] = child_report.failures
    assert isinstance(failure, ForeignKeyFailure)
    assert failure.reason == "BLOCKED_BY_FAILED_DEPENDENCY"
    assert table_exists(live_connection, child_name) is False
    assert read_live_table(live_connection, parent_name) == parent_before


def test_fk_type_mismatch_against_registered_parent_is_blocked_before_execution(
    live_connection, live_tables
):
    """Blocks a FK whose column types mismatch the registered parent before any child SQL runs."""
    parent_name = live_tables("mismatch_parent")
    child_name = live_tables("mismatch_child")

    # Given a child whose foreign key is internally consistent: it is declared against a
    # parent object whose id is an Integer, matching the child's parent_id.
    # But the parent declaration *registered for this sync* under the same
    # qualified name types id as a Long — and Unity Catalog requires each
    # foreign-key column's type to equal the referenced column's type. The
    # engine must block the child before executing anything against it,
    # rather than let the warehouse reject the constraint mid-execution and
    # leave a half-built child behind.
    declared_parent = DeltaTable(
        live_catalog(),
        live_schema(),
        parent_name,
        columns=(Column("id", Integer(), nullable=False),),
        primary_key=("id",),
    )
    registered_parent = DeltaTable(
        live_catalog(),
        live_schema(),
        parent_name,
        columns=(Column("id", Long(), nullable=False),),
        primary_key=("id",),
    )
    child = DeltaTable(
        live_catalog(),
        live_schema(),
        child_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("parent_id", Integer()),
        ),
        primary_key=("id",),
        foreign_keys=(ForeignKey(columns={"parent_id": "id"}, references=declared_parent),),
    )

    # When syncing the child alongside the mismatching registered parent
    with pytest.raises(SyncFailedError) as error:
        build_sql_engine(live_connection).sync(child, registered_parent)

    # Then the child is blocked before execution while the parent still synced
    report = error.value.report
    [child_report] = [
        table_report
        for table_report in report.table_runs
        if table_report.qualified_name.name == child_name
    ]
    # On failure the rendered report is the diagnosis: it shows what the
    # engine actually did against the warehouse instead of blocking.
    assert child_report.status is TableRunStatus.FOREIGN_KEY_FAILED, render_report(report)
    [failure] = child_report.failures
    assert isinstance(failure, ForeignKeyFailure)
    assert failure.reason == "REFERENCED_COLUMN_TYPE_MISMATCH"
    # The child never reached the warehouse: no SQL ran, no table was created.
    assert child_report.execution is None
    assert table_exists(live_connection, child_name) is False
    # The registered parent is healthy and still synced.
    assert read_live_table(live_connection, parent_name)["primary_key"] == ("id",)


def test_unreadable_catalog_surfaces_as_typed_read_failure(live_connection):
    """An unreadable catalog surfaces as a typed read failure, not a raw connector error."""
    # Given a declaration in a catalog that does not exist
    declaration = DeltaTable(
        "de_live_nonexistent_catalog",
        live_schema(),
        "de_live_unreadable",
        columns=(Column("id", Integer()),),
    )

    # When syncing it
    with pytest.raises(SyncFailedError) as error:
        build_sql_engine(live_connection).sync(declaration)

    # Then the failure is a typed read failure in the report — the reader
    # port is total, so a backend error must not escape as a raw connector
    # exception
    [table_report] = error.value.report.table_runs
    assert table_report.status is TableRunStatus.READ_FAILED
    [failure] = table_report.failures
    assert isinstance(failure, ReadFailure)
    assert failure.exception_type
    assert failure.message
