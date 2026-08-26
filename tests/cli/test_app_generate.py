"""Behavioural tests for the ``delta-engine generate`` command."""

from delta_engine.api.codegen import generate_module
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import TablePresent
from delta_engine.cli.app import app
from delta_engine.domain.model import (
    Long,
    ObservedColumn,
    ObservedForeignKeyConstraint,
    ObservedPrimaryKeyConstraint,
    ObservedTable,
    QualifiedName,
)
from tests.cli.conftest import observed_orders


def test_generate_prints_an_importable_module_and_exits_cleanly(runner, fake_reader) -> None:
    # Given a live table
    observed = observed_orders()
    fake_reader.states["dev.silver.orders"] = observed

    # When
    result = runner.invoke(app, ["generate", "dev.silver.orders"])

    # Then the generated module is the entire stdout and stderr stays silent
    assert result.exit_code == 0
    assert result.stdout == generate_module(observed.table).source
    assert result.stderr == ""


def test_generate_reports_dropped_foreign_keys_on_stderr_only(runner, fake_reader) -> None:
    # Given a live table owning a foreign key
    fake_reader.states["dev.silver.orders"] = TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName("dev", "silver", "orders"),
            columns=(
                ObservedColumn("id", Long(), nullable=False),
                ObservedColumn("customer_id", Long()),
            ),
            primary_key=ObservedPrimaryKeyConstraint(("id",), "orders_pk"),
            foreign_keys=(
                ObservedForeignKeyConstraint(
                    local_columns=("customer_id",),
                    referenced_table=QualifiedName("dev", "silver", "customers"),
                    referenced_columns=("id",),
                    name="fk_orders_customer",
                ),
            ),
        )
    )

    # When
    result = runner.invoke(app, ["generate", "dev.silver.orders"])

    # Then the warning goes to stderr and the module stays importable
    assert result.exit_code == 0
    assert "warning: foreign key fk_orders_customer" in result.stderr
    assert "warning" not in result.stdout
    assert "orders = DeltaTable(\n" in result.stdout


def test_generate_fails_when_the_table_does_not_exist(runner, fake_reader) -> None:
    # Given no live table (the fake reader reports absent by default)

    # When
    result = runner.invoke(app, ["generate", "dev.silver.missing"])

    # Then
    assert result.exit_code == 1
    assert "error: table dev.silver.missing does not exist" in result.stderr
    assert result.stdout == ""


def test_generate_fails_when_the_catalog_cannot_be_read(runner, fake_reader) -> None:
    # Given a table whose state cannot be determined
    fake_reader.states["dev.silver.orders"] = ReadError("PermissionError", "no access")

    # When
    result = runner.invoke(app, ["generate", "dev.silver.orders"])

    # Then
    assert result.exit_code == 1
    assert "error: no access" in result.stderr
    assert result.stdout == ""


def test_generate_fails_when_the_table_cannot_be_declared(runner, fake_reader) -> None:
    # Given a live table the engine cannot declare (legacy id column mapping)
    fake_reader.states["dev.silver.legacy"] = TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName("dev", "silver", "legacy"),
            columns=(ObservedColumn("id", Long()),),
            properties={"delta.columnMapping.mode": "id"},
        )
    )

    # When
    result = runner.invoke(app, ["generate", "dev.silver.legacy"])

    # Then
    assert result.exit_code == 1
    assert "error: cannot generate a declaration for dev.silver.legacy" in result.stderr
    assert result.stdout == ""


def test_generate_rejects_a_name_that_is_not_fully_qualified(runner) -> None:
    # Given a name missing its catalog and schema
    table_name = "orders"

    # When
    result = runner.invoke(app, ["generate", table_name])

    # Then no connection is attempted and the mistake is named
    assert result.exit_code == 1
    assert "error:" in result.stderr
    assert "catalog.schema.table" in result.stderr
