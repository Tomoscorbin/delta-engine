"""Live Unity Catalog coverage for primary keys, foreign keys, and dependency ordering."""

import pytest

pytest.importorskip("databricks.sql")

from databricks.sql.exc import ServerOperationError

from delta_engine import (
    SyncFailedError,
    TableRunStatus,
    ValidationFailure,
)
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
    """Adds, widens to composite, and drops a primary key across successive syncs."""
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
    """Creates a composite foreign key parent-first from reversed input, then drops it."""
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
    """Creates a self-referential foreign key on a single table."""
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
    """Adopts a live key created under foreign constraint names, finding no drift."""
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


def test_platform_lowercases_custom_constraint_names_and_drops_them_case_insensitively(
    live_connection, live_tables
):
    """Custom key names are stored lowercase and resolve case-insensitively when dropped."""
    # Constraint name spelling is not identity or retained display metadata.
    # This is the platform boundary behind normalizing an explicitly requested
    # name as a case-insensitive identifier.
    parent_name = live_tables("custom_name_parent")
    child_name = live_tables("custom_name_child")
    primary_key_name = f"{parent_name}_CustomPk"
    foreign_key_name = f"{child_name}_CustomFk"
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(parent_name)} "
        f"(id INT NOT NULL, CONSTRAINT `{primary_key_name}` PRIMARY KEY (id)) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} "
        f"(parent_id INT, CONSTRAINT `{foreign_key_name}` FOREIGN KEY (parent_id) "
        f"REFERENCES {qualified_table(parent_name)} (id)) USING DELTA",
    )

    parent = read_live_table(live_connection, parent_name)
    child = read_live_table(live_connection, child_name)
    assert parent["primary_key_name"] == primary_key_name.lower()
    assert child["foreign_keys"] == ((foreign_key_name.lower(), "parent_id", parent_name, "id"),)

    # DROP CONSTRAINT is how the engine removes an observed FK. A different
    # spelling of the same name resolves, while the engine's name-independent
    # DROP PRIMARY KEY form also works for a custom-named PK.
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(child_name)} "
        f"DROP CONSTRAINT IF EXISTS `{foreign_key_name.swapcase()}`",
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(parent_name)} DROP PRIMARY KEY IF EXISTS",
    )
    assert read_live_table(live_connection, child_name)["foreign_keys"] == ()
    assert read_live_table(live_connection, parent_name)["primary_key"] == ()


def test_platform_generates_names_for_unnamed_primary_and_foreign_keys(
    live_connection, live_tables
):
    """Databricks accepts unnamed keys and exposes the generated names in the catalog."""
    # The engine will continue to materialize its own default names. This pin
    # records that raw SQL callers may omit names without making generated-name
    # shape or stability part of the engine's contract.
    parent_name = live_tables("unnamed_parent")
    child_name = live_tables("unnamed_child")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(parent_name)} "
        "(id INT NOT NULL, PRIMARY KEY (id)) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} "
        f"(parent_id INT, FOREIGN KEY (parent_id) "
        f"REFERENCES {qualified_table(parent_name)} (id)) USING DELTA",
    )

    primary_key_name = read_live_table(live_connection, parent_name)["primary_key_name"]
    [(foreign_key_name, local_column, referenced_table, referenced_column)] = read_live_table(
        live_connection, child_name
    )["foreign_keys"]
    assert isinstance(primary_key_name, str) and primary_key_name
    assert isinstance(foreign_key_name, str) and foreign_key_name
    assert primary_key_name.casefold() != foreign_key_name.casefold()
    assert local_column == "parent_id"
    assert referenced_table == parent_name
    assert referenced_column == "id"


def test_platform_uses_one_case_insensitive_constraint_name_namespace_per_schema(
    live_connection, live_tables
):
    """PK and FK names on different tables still collide case-insensitively within a schema."""
    parent_name = live_tables("constraint_namespace_parent")
    child_name = live_tables("constraint_namespace_child")
    shared_name = f"{parent_name}_SharedConstraint"
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(parent_name)} "
        f"(id INT NOT NULL, CONSTRAINT `{shared_name}` PRIMARY KEY (id)) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} (parent_id INT) USING DELTA",
    )

    # The conflicting name belongs to a different constraint kind and table;
    # swapped case proves the namespace compares identifiers, not raw strings.
    with pytest.raises(ServerOperationError):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(child_name)} "
            f"ADD CONSTRAINT `{shared_name.swapcase()}` FOREIGN KEY (parent_id) "
            f"REFERENCES {qualified_table(parent_name)} (id)",
        )

    assert read_live_table(live_connection, child_name)["foreign_keys"] == ()


def test_platform_does_not_offer_a_direct_constraint_rename_clause(live_connection, live_tables):
    """Constraint names change only by dropping and recreating the constraint."""
    table_name = live_tables("constraint_rename")
    old_name = f"{table_name}_OldPk"
    new_name = f"{table_name}_NewPk"
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} "
        f"(id INT NOT NULL, CONSTRAINT `{old_name}` PRIMARY KEY (id)) USING DELTA",
    )

    with pytest.raises(ServerOperationError):
        execute_sql(
            live_connection,
            f"ALTER TABLE {qualified_table(table_name)} "
            f"RENAME CONSTRAINT `{old_name}` TO `{new_name}`",
        )

    assert read_live_table(live_connection, table_name)["primary_key_name"] == old_name.lower()


def test_primary_key_drop_is_not_blocked_by_unique_backed_foreign_keys(
    live_connection, live_tables
):
    """Drops a primary key even while a UNIQUE constraint outside the model backs an FK."""
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


def _camel_case_pk_declaration(table_name, column_name, key_column):
    return DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=table_name,
        columns=(Column(column_name, String(), nullable=False),),
        primary_key=(key_column,),
        scope="metadata",
    )


def test_primary_key_on_a_camel_case_column_converges(live_connection, live_tables):
    """A primary key declared with the catalog's exact camelCase spelling applies and settles."""
    # The managed-constraint path is case-sensitive about physical column
    # spelling, unlike ordinary ALTER COLUMN (pinned in
    # test_sql_warehouse_live_platform_assumptions.py), so a camelCase column
    # needs live proof that the key compiles and executes with that spelling.

    # Given a camelCase catalog column declared with the same spelling
    table_name = live_tables("column_case_add_primary_key")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (`requestId` STRING NOT NULL) USING DELTA",
    )
    declaration = _camel_case_pk_declaration(table_name, "requestId", "requestId")
    engine = build_sql_engine(live_connection)

    # When syncing
    report = engine.sync(declaration)

    # Then the key statement, the live state, and the re-sync all carry
    # the catalog spelling
    assert report.has_failures is False
    statements = next(iter(report.planned_sql_statements.values()))
    assert statements == (
        f"ALTER TABLE {qualified_table(table_name)} "
        f"ADD CONSTRAINT `{table_name}_pk` PRIMARY KEY (`requestId`)",
    )
    state = read_live_table(live_connection, table_name)
    assert state["primary_key"] == ("requestId",)
    assert state["primary_key_name"] == f"{table_name}_pk"
    assert engine.sync(declaration).has_changes is False


def test_primary_key_declared_in_the_wrong_case_is_rejected(live_connection, live_tables):
    """Declaring a camelCase column lowercase is rejected without touching the catalog."""
    # Exact spelling is a law, not a suppressible rule, and it holds at every
    # scope: a metadata-scoped declaration still names the columns it keys, so
    # a misspelled name is a defect in the declaration rather than unmanaged
    # column-structure drift. This is the production shape — adding a key to a
    # table someone else created, whose camelCase spelling is easy to get wrong.

    # Given a camelCase catalog column declared lowercase
    table_name = live_tables("column_case_wrong_case_pk")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(table_name)} (`requestId` STRING NOT NULL) USING DELTA",
    )
    declaration = _camel_case_pk_declaration(table_name, "requestid", "requestid")
    before = read_live_table(live_connection, table_name)

    # When syncing
    with pytest.raises(SyncFailedError) as error:
        build_sql_engine(live_connection).sync(declaration)

    # Then the rejection names the rule and both spellings, nothing was
    # planned, and the live table is untouched
    [table_report] = error.value.report.table_runs
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    [failure] = table_report.failures
    assert isinstance(failure, ValidationFailure)
    assert failure.rule_name == "ColumnSpellingMustMatchCatalog"
    assert "'requestid'" in failure.message
    assert "'requestId'" in failure.message
    assert table_report.compiled is None
    assert read_live_table(live_connection, table_name) == before


def _camel_case_fk_pair(parent_name, child_name, *, parent_column, child_column):
    parent = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=parent_name,
        columns=(Column(parent_column, String(), nullable=False),),
        primary_key=(parent_column,),
        scope="metadata",
    )
    child = DeltaTable(
        catalog=live_catalog(),
        schema=live_schema(),
        name=child_name,
        columns=(Column(child_column, String()),),
        foreign_keys=(ForeignKey(columns={child_column: parent_column}, references=parent),),
        scope="metadata",
    )
    return parent, child


def _create_camel_case_fk_tables(live_connection, parent_name, child_name):
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(parent_name)} "
        f"(`orderId` STRING NOT NULL, CONSTRAINT `{parent_name}_pk` "
        "PRIMARY KEY (`orderId`)) USING DELTA",
    )
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(child_name)} (`orderRef` STRING) USING DELTA",
    )


def test_foreign_key_on_camel_case_columns_converges(live_connection, live_tables):
    """An FK across camelCase columns applies and settles when both sides are spelled exactly."""
    # Given camelCase child and parent columns in the catalog, declared with
    # the same spelling on both sides
    parent_name = live_tables("column_case_fk_parent")
    child_name = live_tables("column_case_fk_child")
    _create_camel_case_fk_tables(live_connection, parent_name, child_name)
    parent, child = _camel_case_fk_pair(
        parent_name, child_name, parent_column="orderId", child_column="orderRef"
    )
    engine = build_sql_engine(live_connection)

    # When syncing both tables
    report = engine.sync(child, parent)

    # Then the constraint statement, the live state, and the re-sync all
    # spell both sides exactly as the catalog does
    assert report.has_failures is False
    assert report.planned_sql_statements[f"{live_catalog()}.{live_schema()}.{child_name}"] == (
        f"ALTER TABLE {qualified_table(child_name)} "
        f"ADD CONSTRAINT `{child_name}_orderref_fk` FOREIGN KEY (`orderRef`) "
        f"REFERENCES {qualified_table(parent_name)} (`orderId`)",
    )
    assert read_live_table(live_connection, child_name)["foreign_keys"] == (
        (f"{child_name}_orderref_fk", "orderRef", parent_name, "orderId"),
    )
    assert engine.sync(child, parent).has_changes is False


def test_foreign_key_declared_in_the_wrong_case_is_rejected(live_connection, live_tables):
    """Scrambled casing on either side of an FK is rejected without touching the catalog."""
    # Both tables are judged independently, so both name their own misspelling;
    # neither reaches execution, so neither constraint is created.

    # Given camelCase child and parent columns declared with scrambled casing
    parent_name = live_tables("column_case_wrong_case_fk_parent")
    child_name = live_tables("column_case_wrong_case_fk_child")
    _create_camel_case_fk_tables(live_connection, parent_name, child_name)
    parent, child = _camel_case_fk_pair(
        parent_name, child_name, parent_column="orderid", child_column="orderref"
    )
    parent_before = read_live_table(live_connection, parent_name)
    child_before = read_live_table(live_connection, child_name)

    # When syncing both tables
    with pytest.raises(SyncFailedError) as error:
        build_sql_engine(live_connection).sync(child, parent)

    # Then each table names its own spelling defect and no DDL ran
    reports = {str(report.qualified_name): report for report in error.value.report.table_runs}
    for name, declared, observed in (
        (parent_name, "orderid", "orderId"),
        (child_name, "orderref", "orderRef"),
    ):
        table_report = reports[f"{live_catalog()}.{live_schema()}.{name}"]
        assert table_report.status is TableRunStatus.PLANNING_FAILED
        [failure] = table_report.failures
        assert isinstance(failure, ValidationFailure)
        assert failure.rule_name == "ColumnSpellingMustMatchCatalog"
        assert f"'{declared}'" in failure.message
        assert f"'{observed}'" in failure.message
        assert table_report.compiled is None
    assert read_live_table(live_connection, parent_name) == parent_before
    assert read_live_table(live_connection, child_name) == child_before
