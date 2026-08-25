"""Live round trip for declaration generation: create, generate, diff clean."""

from delta_engine.adapters.databricks.warehouse.factory import build_reader
from delta_engine.api.codegen import generate_module
from delta_engine.api.delta_table import DeltaTable
from delta_engine.application.ports import TablePresent
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import diff_table
from tests.live.sql_warehouse_live_helpers import (
    execute_sql,
    live_catalog,
    live_schema,
    qualified_table,
)


def test_a_generated_declaration_matches_the_live_table_exactly(
    live_connection, live_tables
) -> None:
    """A generated module imports and plans no changes against its source table."""
    # Given a live table with a named primary key, comments, and a tag
    name = live_tables("generate")
    execute_sql(
        live_connection,
        f"CREATE TABLE {qualified_table(name)} ("
        " id BIGINT NOT NULL COMMENT 'Order key',"
        " note STRING,"
        f" CONSTRAINT {name}_pk PRIMARY KEY (id)"
        ") COMMENT 'Generate round trip.'",
    )
    execute_sql(
        live_connection,
        f"ALTER TABLE {qualified_table(name)} SET TAGS ('tier' = 'gold')",
    )

    # When the observed state is generated and imported back
    reader = build_reader(live_connection)
    state = reader.fetch_state(QualifiedName(live_catalog(), live_schema(), name))
    assert isinstance(state, TablePresent)
    module = generate_module(state.table)

    namespace: dict[str, object] = {}
    exec(compile(module.source, "<generated>", "exec"), namespace)
    (declared,) = namespace["all_tables"]  # type: ignore[misc]
    assert isinstance(declared, DeltaTable)

    # Then the declaration matches the live table exactly
    diff = diff_table(declared.to_desired_table(), state.table)
    assert diff.actions == ()
    assert diff.unresolvable == ()
    assert module.warnings == ()
