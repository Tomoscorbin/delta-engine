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
    execute_sql,
    fetch_rows,
    live_catalog,
    live_schema,
    qualified_table,
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


def test_structurally_matching_constraints_adopt_foreign_names_without_drift(
    live_connection, live_tables
):
    # Constraint identity is structural: a live PK/FK created under names the
    # engine would never generate is still the declared constraint, so a sync
    # finds no drift and the foreign names survive.
    table_name = live_tables("adopted_names")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (id INT NOT NULL, manager_id INT, "
        f"CONSTRAINT {table_name}_legacy_pk PRIMARY KEY (id)) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(table_name)} ADD CONSTRAINT {table_name}_legacy_fk "
        f"FOREIGN KEY (manager_id) REFERENCES {qualified_table(table_name)} (id)",
    )
    declaration = DeltaTable(
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

    report = build_sql_engine(live_connection).sync(declaration)

    assert report.has_failures is False
    assert report.has_changes is False
    state = read_live_table(live_connection, table_name)
    assert state["primary_key_name"] == f"{table_name}_legacy_pk"
    assert state["foreign_keys"] == ((f"{table_name}_legacy_fk", "manager_id", table_name, "id"),)


def test_primary_key_drop_is_not_blocked_by_unique_backed_foreign_keys(
    live_connection, live_tables
):
    # given a parent whose primary key has no referencing foreign keys, but
    # whose UNIQUE constraint backs one (UNIQUE constraints are DBR 18.2+
    # Public Preview and outside the engine's model)
    parent_name = live_tables("uq_parent")
    child_name = live_tables("uq_child")
    parent_columns = (
        Column("id", Integer(), nullable=False),
        Column("email", String(), nullable=False),
    )
    engine = build_sql_engine(live_connection)
    engine.sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            parent_name,
            columns=parent_columns,
            primary_key=("id",),
        )
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(parent_name)} "
        f"ADD CONSTRAINT {parent_name}_uq UNIQUE (email)",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} (parent_email STRING, "
        f"CONSTRAINT {child_name}_fk FOREIGN KEY (parent_email) "
        f"REFERENCES {qualified_table(parent_name)} (email)) USING DELTA",
    )
    constraint_types = {
        row["constraint_type"]
        for row in fetch_rows(
            live_connection,
            f"SELECT constraint_type FROM `{live_catalog()}`.information_schema.table_constraints"
            f" WHERE table_schema = '{live_schema()}' AND table_name = '{parent_name}'",
        )
    }
    assert constraint_types == {"PRIMARY KEY", "UNIQUE"}

    # when the declaration drops the primary key
    engine.sync(DeltaTable(live_catalog(), live_schema(), parent_name, columns=parent_columns))

    # then the unique-backed foreign key neither blocked the drop nor was harmed
    assert read_live_table(live_connection, parent_name)["primary_key"] == ()
    assert read_live_table(live_connection, child_name)["foreign_keys"] == (
        (f"{child_name}_fk", "parent_email", parent_name, "email"),
    )
