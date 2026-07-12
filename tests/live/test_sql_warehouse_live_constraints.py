"""Live Unity Catalog coverage for primary keys, foreign keys, and dependency ordering."""

import pytest

pytest.importorskip("databricks.sql")

from delta_engine.databricks import build_sql_engine
from delta_engine.schema import (
    Column,
    DeltaTable,
    ForeignKey,
    Integer,
    Self,
    String,
)
from tests.live.sql_warehouse_live_helpers import (
    live_catalog,
    live_schema,
    read_live_table,
)


def test_sync_adds_changes_and_drops_primary_key(live_connection, live_tables):
    table_name = live_tables("pk_lifecycle")
    columns = (
        Column("tenant_id", Integer(), nullable=False),
        Column("id", Integer(), nullable=False),
        Column("name", String()),
    )
    engine = build_sql_engine(live_connection)
    engine.sync(DeltaTable(live_catalog(), live_schema(), table_name, columns=columns))
    assert read_live_table(live_connection, table_name)["primary_key"] == ()

    engine.sync(
        DeltaTable(live_catalog(), live_schema(), table_name, columns=columns, primary_key=("id",))
    )
    state = read_live_table(live_connection, table_name)
    assert state["primary_key"] == ("id",)
    assert state["primary_key_name"] == f"{table_name}_pk"

    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            table_name,
            columns=columns,
            primary_key=("tenant_id", "id"),
        )
    )
    assert read_live_table(live_connection, table_name)["primary_key"] == (
        "tenant_id",
        "id",
    )

    engine.sync(DeltaTable(live_catalog(), live_schema(), table_name, columns=columns))
    assert read_live_table(live_connection, table_name)["primary_key"] == ()


def test_sync_creates_composite_foreign_key_in_dependency_order_and_removes_it(
    live_connection, live_tables
):
    parent_name = live_tables("accounts")
    child_name = live_tables("orders")
    parent_columns = (
        Column("tenant_id", Integer(), nullable=False),
        Column("account_id", Integer(), nullable=False),
        Column("name", String()),
    )
    parent = DeltaTable(
        live_catalog(),
        live_schema(),
        parent_name,
        columns=parent_columns,
        primary_key=("tenant_id", "account_id"),
    )
    child_columns = (
        Column("id", Integer(), nullable=False),
        Column("tenant_id", Integer()),
        Column("account_id", Integer()),
    )
    child = DeltaTable(
        live_catalog(),
        live_schema(),
        child_name,
        columns=child_columns,
        primary_key=("id",),
        foreign_keys=(
            ForeignKey(
                columns={"tenant_id": "tenant_id", "account_id": "account_id"},
                references=parent,
            ),
        ),
    )
    engine = build_sql_engine(live_connection)

    # Reverse input is deliberate: dependency resolution must still create the parent first.
    engine.sync(child, parent)

    assert read_live_table(live_connection, parent_name)["primary_key"] == (
        "tenant_id",
        "account_id",
    )
    assert read_live_table(live_connection, child_name)["foreign_keys"] == (
        (f"{child_name}_account_id_tenant_id_fk", "account_id", parent_name, "account_id"),
        (f"{child_name}_account_id_tenant_id_fk", "tenant_id", parent_name, "tenant_id"),
    )

    child_without_fk = DeltaTable(
        live_catalog(),
        live_schema(),
        child_name,
        columns=child_columns,
        primary_key=("id",),
    )
    engine.sync(child_without_fk)
    assert read_live_table(live_connection, child_name)["foreign_keys"] == ()

    parent_without_pk = DeltaTable(
        live_catalog(), live_schema(), parent_name, columns=parent_columns
    )
    engine.sync(parent_without_pk)
    assert read_live_table(live_connection, parent_name)["primary_key"] == ()


def test_sync_creates_self_referential_foreign_key(live_connection, live_tables):
    table_name = live_tables("employees")
    table = DeltaTable(
        live_catalog(),
        live_schema(),
        table_name,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("manager_id", Integer()),
        ),
        primary_key=("id",),
        foreign_keys=(ForeignKey(columns={"manager_id": "id"}, references=Self),),
    )

    build_sql_engine(live_connection).sync(table)

    assert read_live_table(live_connection, table_name)["foreign_keys"] == (
        (f"{table_name}_manager_id_fk", "manager_id", table_name, "id"),
    )
