"""
Live Unity Catalog coverage for declarative column renames.

These tests prove the rename contract end to end through ``Engine.sync``:
aspects the rename preserves (data, tags, comments, partition layout)
converge in a single sync with no healing pass, keys the rename would
destroy are replaced explicitly in the same plan, and the two undecidable
states (ambiguous rename, referenced primary key) are rejected without
touching the catalog.
"""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine import SyncFailedError, TableRunStatus, ValidationFailure
from delta_engine.databricks import build_sql_engine
from delta_engine.schema import (
    Column,
    DeltaTable,
    ForeignKey,
    Integer,
    Property,
    String,
)
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    fetch_rows,
    live_catalog,
    live_schema,
    qualified_table,
    read_live_table,
)

_COLUMN_MAPPING = {Property.COLUMN_MAPPING_MODE: "name"}


def test_sync_renames_a_column_preserving_data_tags_and_comment(live_connection, live_tables):
    """Renaming a column carries its data, tags, and comment, converging in one sync."""
    table_name = live_tables("rename_travel")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("id", Integer(), nullable=False),
                Column("customer_nm", String(), comment="the name", tags={"pii": "true"}),
            ),
            properties=_COLUMN_MAPPING,
        )
    )
    execute_sql(
        live_connection,
        f"INSERT INTO {qualified_table(table_name)} VALUES (1, 'ada'), (2, 'grace')",
    )

    renamed = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column(
                "customer_name",
                String(),
                comment="the name",
                tags={"pii": "true"},
                renamed_from="customer_nm",
            ),
        ),
        properties=_COLUMN_MAPPING,
    )
    engine.sync(renamed)

    state = read_live_table(live_connection, table_name)
    assert [column["column_name"] for column in state["columns"]] == ["id", "customer_name"]
    [name_column] = [c for c in state["columns"] if c["column_name"] == "customer_name"]
    assert name_column["comment"] == "the name"
    assert state["column_tags"] == {("customer_name", "pii"): "true"}
    rows = fetch_rows(
        live_connection,
        f"SELECT id, customer_name FROM {qualified_table(table_name)} ORDER BY id",
    )
    assert [(row["id"], row["customer_name"]) for row in rows] == [(1, "ada"), (2, "grace")]

    # Everything travelled with the rename, so one sync converged: the same
    # declaration finds no drift and the applied hint is inert.
    assert engine.sync(renamed).has_changes is False


def test_sync_replaces_a_primary_key_across_a_rename_in_one_plan(live_connection, live_tables):
    """Renaming a primary-key column replaces the key explicitly in one plan."""
    table_name = live_tables("rename_pk")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("customer_nm", String(), nullable=False),
                Column("amount", Integer()),
            ),
            primary_key=("customer_nm",),
            properties=_COLUMN_MAPPING,
        )
    )
    assert read_live_table(live_connection, table_name)["primary_key"] == ("customer_nm",)

    renamed = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("customer_name", String(), nullable=False, renamed_from="customer_nm"),
            Column("amount", Integer()),
        ),
        primary_key=("customer_name",),
        properties=_COLUMN_MAPPING,
    )
    report = engine.sync(renamed)

    # The plan is a complete transcript: the key is dropped and re-added
    # explicitly around the rename rather than relying on the implicit drop
    # Databricks performs during RENAME COLUMN.
    [table_report] = report.table_runs
    assert table_report.compiled is not None
    statements = table_report.compiled.statements
    assert len(statements) == 3
    assert "DROP PRIMARY KEY" in statements[0]
    assert "RENAME COLUMN" in statements[1]
    assert "ADD PRIMARY KEY" in statements[2]
    state = read_live_table(live_connection, table_name)
    assert state["primary_key"] == ("customer_name",)
    assert isinstance(state["primary_key_name"], str) and state["primary_key_name"]
    assert engine.sync(renamed).has_changes is False


def test_sync_replaces_a_composite_primary_key_when_one_member_is_renamed(
    live_connection, live_tables
):
    """Renaming one member of a composite primary key replaces the whole key."""
    table_name = live_tables("rename_composite_pk")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(
                Column("tenant_id", Integer(), nullable=False),
                Column("customer_nm", String(), nullable=False),
                Column("amount", Integer()),
            ),
            primary_key=("tenant_id", "customer_nm"),
            properties=_COLUMN_MAPPING,
        )
    )
    assert read_live_table(live_connection, table_name)["primary_key"] == (
        "tenant_id",
        "customer_nm",
    )

    renamed = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("tenant_id", Integer(), nullable=False),
            Column("customer_name", String(), nullable=False, renamed_from="customer_nm"),
            Column("amount", Integer()),
        ),
        primary_key=("tenant_id", "customer_name"),
        properties=_COLUMN_MAPPING,
    )
    report = engine.sync(renamed)

    # Renaming one member still replaces the whole key: the drop and re-add
    # bracket the rename, and the untouched member keeps its position.
    [table_report] = report.table_runs
    assert table_report.compiled is not None
    statements = table_report.compiled.statements
    assert len(statements) == 3
    assert "DROP PRIMARY KEY" in statements[0]
    assert "RENAME COLUMN" in statements[1]
    assert "ADD PRIMARY KEY" in statements[2]
    state = read_live_table(live_connection, table_name)
    assert state["primary_key"] == ("tenant_id", "customer_name")
    assert isinstance(state["primary_key_name"], str) and state["primary_key_name"]
    assert engine.sync(renamed).has_changes is False


def test_sync_replaces_a_foreign_key_across_a_local_column_rename(live_connection, live_tables):
    """Renaming a local foreign-key column replaces the foreign key in one plan."""
    parent_name = live_tables("rename_fk_parent")
    child_name = live_tables("rename_fk_child")
    parent = DeltaTable(
        live_catalog(),
        live_schema(),
        parent_name,
        columns=(Column("id", Integer(), nullable=False),),
        primary_key=("id",),
    )
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            child_name,
            columns=(Column("parent", Integer()),),
            foreign_keys=(ForeignKey(columns={"parent": "id"}, references=parent),),
            properties=_COLUMN_MAPPING,
        ),
        parent,
    )

    renamed_child = DeltaTable(
        live_catalog(),
        live_schema(),
        child_name,
        columns=(Column("parent_id", Integer(), renamed_from="parent"),),
        foreign_keys=(ForeignKey(columns={"parent_id": "id"}, references=parent),),
        properties=_COLUMN_MAPPING,
    )
    # The parent rides along: dependency resolution blocks a foreign-key add
    # whose referenced table is not registered in the same run.
    engine.sync(renamed_child, parent)

    [(constraint_name, local_column, referenced_table, referenced_column)] = read_live_table(
        live_connection, child_name
    )["foreign_keys"]
    assert isinstance(constraint_name, str) and constraint_name
    assert (local_column, referenced_table, referenced_column) == (
        "parent_id",
        parent_name,
        "id",
    )
    assert engine.sync(renamed_child, parent).has_changes is False


def test_sync_replaces_a_composite_foreign_key_when_one_local_column_is_renamed(
    live_connection, live_tables
):
    """Renaming one local column of a composite foreign key replaces the whole key."""
    parent_name = live_tables("rename_composite_fk_parent")
    child_name = live_tables("rename_composite_fk_child")
    parent = DeltaTable(
        live_catalog(),
        live_schema(),
        parent_name,
        columns=(
            Column("tenant_id", Integer(), nullable=False),
            Column("account_id", Integer(), nullable=False),
        ),
        primary_key=("tenant_id", "account_id"),
    )
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            child_name,
            columns=(Column("tenant_id", Integer()), Column("account_num", Integer())),
            foreign_keys=(
                ForeignKey(
                    columns={"tenant_id": "tenant_id", "account_num": "account_id"},
                    references=parent,
                ),
            ),
            properties=_COLUMN_MAPPING,
        ),
        parent,
    )
    original_foreign_keys = read_live_table(live_connection, child_name)["foreign_keys"]
    original_name = original_foreign_keys[0][0]
    assert isinstance(original_name, str) and original_name
    assert original_foreign_keys == (
        (original_name, "account_num", parent_name, "account_id"),
        (original_name, "tenant_id", parent_name, "tenant_id"),
    )

    renamed_child = DeltaTable(
        live_catalog(),
        live_schema(),
        child_name,
        columns=(
            Column("tenant_id", Integer()),
            Column("account_id", Integer(), renamed_from="account_num"),
        ),
        foreign_keys=(
            ForeignKey(
                columns={"tenant_id": "tenant_id", "account_id": "account_id"},
                references=parent,
            ),
        ),
        properties=_COLUMN_MAPPING,
    )
    # The parent rides along: dependency resolution blocks a foreign-key add
    # whose referenced table is not registered in the same run.
    engine.sync(renamed_child, parent)

    # Renaming one local column replaces the whole composite key: the new
    # local column set produces a new occurrence and both pairs survive intact.
    renamed_foreign_keys = read_live_table(live_connection, child_name)["foreign_keys"]
    renamed_name = renamed_foreign_keys[0][0]
    assert isinstance(renamed_name, str) and renamed_name
    assert renamed_foreign_keys == (
        (renamed_name, "account_id", parent_name, "account_id"),
        (renamed_name, "tenant_id", parent_name, "tenant_id"),
    )
    assert engine.sync(renamed_child, parent).has_changes is False


def test_sync_rejects_a_primary_key_rename_referenced_by_a_foreign_key(
    live_connection, live_tables
):
    """Renaming a primary key an FK references is rejected, touching neither table."""
    parent_name = live_tables("rename_blocked_parent")
    child_name = live_tables("rename_blocked_child")
    parent = DeltaTable(
        live_catalog(),
        live_schema(),
        parent_name,
        columns=(Column("id", Integer(), nullable=False),),
        primary_key=("id",),
        properties=_COLUMN_MAPPING,
    )
    child = DeltaTable(
        live_catalog(),
        live_schema(),
        child_name,
        columns=(Column("parent_id", Integer()),),
        foreign_keys=(ForeignKey(columns={"parent_id": "id"}, references=parent),),
    )
    engine = build_sql_engine(live_connection)
    engine.sync(child, parent)
    parent_before = read_live_table(live_connection, parent_name)
    child_before = read_live_table(live_connection, child_name)

    with pytest.raises(SyncFailedError) as error:
        engine.sync(
            DeltaTable(
                live_catalog(),
                live_schema(),
                parent_name,
                columns=(Column("account_id", Integer(), nullable=False, renamed_from="id"),),
                primary_key=("account_id",),
                properties=_COLUMN_MAPPING,
            )
        )

    # Renaming the key column would let RENAME COLUMN silently drop the
    # child's foreign key, so the engine refuses client-side: nothing was
    # planned and neither table changed.
    [parent_report] = error.value.report.table_runs
    assert parent_report.status is TableRunStatus.PLANNING_FAILED
    [failure] = parent_report.failures
    assert isinstance(failure, ValidationFailure)
    assert failure.rule_name == "PrimaryKeyReferencedByForeignKeys"
    assert parent_report.compiled is None
    assert read_live_table(live_connection, parent_name) == parent_before
    assert read_live_table(live_connection, child_name) == child_before


def test_sync_rejects_an_ambiguous_rename_without_catalog_change(live_connection, live_tables):
    """An ambiguous column rename is rejected, and the catalog is left unchanged."""
    table_name = live_tables("rename_ambiguous")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("customer_nm", String()), Column("customer_name", String())),
            properties=_COLUMN_MAPPING,
        )
    )
    before = read_live_table(live_connection, table_name)

    with pytest.raises(SyncFailedError) as error:
        engine.sync(
            DeltaTable(
                live_catalog(),
                live_schema(),
                table_name,
                columns=(Column("customer_name", String(), renamed_from="customer_nm"),),
                properties=_COLUMN_MAPPING,
            )
        )

    [table_report] = error.value.report.table_runs
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert "AmbiguousColumnRename" in {
        failure.rule_name
        for failure in table_report.failures
        if isinstance(failure, ValidationFailure)
    }
    assert read_live_table(live_connection, table_name) == before


def test_sync_renames_a_partition_column_without_layout_drift(live_connection, live_tables):
    """Renaming a partition column carries the layout with no partitioning drift."""
    table_name = live_tables("rename_partition")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()), Column("day", String())),
            partitioned_by=("day",),
            properties=_COLUMN_MAPPING,
        )
    )
    execute_sql(
        live_connection,
        f"INSERT INTO {qualified_table(table_name)} VALUES (1, 'mon'), (2, 'tue')",
    )

    renamed = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer()), Column("event_day", String(), renamed_from="day")),
        partitioned_by=("event_day",),
        properties=_COLUMN_MAPPING,
    )
    report = engine.sync(renamed)

    # Partition metadata follows the mapped column's identity, so the whole
    # plan is the rename itself — no layout action, no partitioning drift.
    [table_report] = report.table_runs
    assert table_report.compiled is not None
    [statement] = table_report.compiled.statements
    assert "RENAME COLUMN" in statement
    state = read_live_table(live_connection, table_name)
    assert state["partitioning"] == ("event_day",)
    rows = fetch_rows(
        live_connection,
        f"SELECT event_day FROM {qualified_table(table_name)} ORDER BY id",
    )
    assert [row["event_day"] for row in rows] == ["mon", "tue"]
    assert engine.sync(renamed).has_changes is False


def test_sync_renames_a_clustering_key_without_layout_drift(live_connection, live_tables):
    """Renaming a clustering key carries the layout with no clustering drift."""
    table_name = live_tables("rename_cluster")
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=(Column("id", Integer()), Column("region", String())),
            clustered_by=("region",),
            properties=_COLUMN_MAPPING,
        )
    )
    execute_sql(
        live_connection,
        f"INSERT INTO {qualified_table(table_name)} VALUES (1, 'emea'), (2, 'apac')",
    )

    renamed = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(Column("id", Integer()), Column("sales_region", String(), renamed_from="region")),
        clustered_by=("sales_region",),
        properties=_COLUMN_MAPPING,
    )
    report = engine.sync(renamed)

    # Clustering keys follow the mapped column's identity, so the whole plan
    # is the rename itself — no re-clustering action, no layout drift.
    [table_report] = report.table_runs
    assert table_report.compiled is not None
    [statement] = table_report.compiled.statements
    assert "RENAME COLUMN" in statement
    state = read_live_table(live_connection, table_name)
    assert state["clustering"] == ("sales_region",)
    rows = fetch_rows(
        live_connection,
        f"SELECT sales_region FROM {qualified_table(table_name)} ORDER BY id",
    )
    assert [row["sales_region"] for row in rows] == ["emea", "apac"]
    assert engine.sync(renamed).has_changes is False


def test_declaration_with_a_rename_hint_creates_a_fresh_table_under_the_new_name(
    live_connection, live_tables
):
    """A rename hint on a fresh catalog is inert; the table is created under the new name."""
    table_name = live_tables("rename_fresh")
    declaration = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("customer_name", String(), renamed_from="customer_nm"),
        ),
        properties=_COLUMN_MAPPING,
    )

    build_sql_engine(live_connection).sync(declaration)

    # Neither name is observed on a fresh catalog, so the hint is inert and
    # the table is simply created under the declared name.
    state = read_live_table(live_connection, table_name)
    assert [column["column_name"] for column in state["columns"]] == ["id", "customer_name"]
