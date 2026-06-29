"""
Unit tests for foreign_key_planning.resolve().

These exercise resolve() directly: dependency-first ordering, cycle
detection, and per-FK skip classification (CYCLE / UNRESOLVABLE_REFERENCE).
"""

from delta_engine.api import Column, DeltaTable, String
from delta_engine.application.foreign_key_planning import resolve
from delta_engine.application.results import SkipReason
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.table import DesiredTable


def _table(fqn: str) -> DesiredTable:
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog, schema, name, columns=(Column("id", String()),)
    ).to_desired_table()


def _table_with_fk(fqn: str, references: str) -> DesiredTable:
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog,
        schema,
        name,
        columns=(Column("id", String()), Column("ref_id", String())),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("ref_id",),
                references=references,
                referenced_columns=("id",),
            )
        ],
    ).to_desired_table()


def test_resolve_with_no_fks_preserves_registry_order():
    # Given three tables with no FKs
    tables = (
        _table("cat.sch.a"),
        _table("cat.sch.b"),
        _table("cat.sch.c"),
    )

    # When
    plan = resolve(tables)

    # Then order is unchanged and nothing is skipped
    names = [str(table.qualified_name) for table in plan.ordered_tables]
    assert names == ["cat.sch.a", "cat.sch.b", "cat.sch.c"]
    assert plan.skipped_foreign_keys == ()


def test_resolve_orders_referenced_table_before_dependent():
    # Given orders depends on customers
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When
    plan = resolve(tables)

    # Then customers appears before orders
    names = [str(table.qualified_name) for table in plan.ordered_tables]
    assert names.index("cat.sch.customers") < names.index("cat.sch.orders")


def test_resolve_handles_chain_of_dependencies():
    # Given c -> b -> a (a must sync first, then b, then c)
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table("cat.sch.a"),
    )

    # When
    plan = resolve(tables)

    # Then a before b before c
    names = [str(table.qualified_name) for table in plan.ordered_tables]
    assert names.index("cat.sch.a") < names.index("cat.sch.b") < names.index("cat.sch.c")


def test_resolve_classifies_unresolvable_reference_as_skipped():
    # Given orders references customers but customers is not registered
    tables = (_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When
    plan = resolve(tables)

    # Then the FK is skipped with UNRESOLVABLE_REFERENCE
    assert len(plan.skipped_foreign_keys) == 1
    skipped = plan.skipped_foreign_keys[0]
    assert skipped.reason == SkipReason.UNRESOLVABLE_REFERENCE
    assert skipped.table.name == "orders"
    assert skipped.constraint_name == "orders_ref_id_fk"


def test_resolve_classifies_cycle_members_as_skipped():
    # Given a -> b and b -> a (mutual cycle)
    table_a = _table_with_fk("cat.sch.a", "cat.sch.b")
    table_b = _table_with_fk("cat.sch.b", "cat.sch.a")

    # When
    plan = resolve((table_a, table_b))

    # Then both FKs are skipped with CYCLE
    assert len(plan.skipped_foreign_keys) == 2
    assert all(skipped.reason == SkipReason.CYCLE for skipped in plan.skipped_foreign_keys)


def test_resolve_includes_cycle_tables_in_ordered_tables():
    # Given a mutual cycle between a and b
    table_a = _table_with_fk("cat.sch.a", "cat.sch.b")
    table_b = _table_with_fk("cat.sch.b", "cat.sch.a")

    # When
    plan = resolve((table_a, table_b))

    # Then both tables still appear in ordered_tables (their non-FK changes still sync)
    names = {str(table.qualified_name) for table in plan.ordered_tables}
    assert names == {"cat.sch.a", "cat.sch.b"}


def test_resolve_does_not_skip_fk_of_table_that_merely_depends_on_a_cycle():
    # Given b <-> c form a mutual cycle, and a depends on b (a is not in the cycle)
    table_a = _table_with_fk("cat.sch.a", "cat.sch.b")
    table_b = _table_with_fk("cat.sch.b", "cat.sch.c")
    table_c = _table_with_fk("cat.sch.c", "cat.sch.b")

    # When
    plan = resolve((table_a, table_b, table_c))

    # Then only the true cycle members b and c are skipped; a's FK survives
    skipped_tables = {str(skipped.table) for skipped in plan.skipped_foreign_keys}
    assert skipped_tables == {"cat.sch.b", "cat.sch.c"}
    assert all(skipped.reason == SkipReason.CYCLE for skipped in plan.skipped_foreign_keys)


def test_resolve_orders_table_after_the_cycle_member_it_depends_on():
    # Given b <-> c (cycle) and a depends on b
    table_a = _table_with_fk("cat.sch.a", "cat.sch.b")
    table_b = _table_with_fk("cat.sch.b", "cat.sch.c")
    table_c = _table_with_fk("cat.sch.c", "cat.sch.b")

    # When
    plan = resolve((table_a, table_b, table_c))

    # Then all three tables are ordered, and b (a's valid dependency) precedes a
    names = [str(table.qualified_name) for table in plan.ordered_tables]
    assert set(names) == {"cat.sch.a", "cat.sch.b", "cat.sch.c"}
    assert names.index("cat.sch.b") < names.index("cat.sch.a")


def test_resolve_treats_self_referential_fk_as_applicable():
    # Given a table whose foreign key references itself (a self-loop)
    table = DeltaTable(
        "cat",
        "sch",
        "employees",
        columns=(Column("id", String()), Column("manager_id", String())),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("manager_id",),
                references="cat.sch.employees",
                referenced_columns=("id",),
            )
        ],
    ).to_desired_table()

    # When
    plan = resolve((table,))

    # Then the self-referencing FK is NOT skipped — it is applied after the table
    # is created — and the table still appears in the ordered output
    assert plan.skipped_foreign_keys == ()
    assert {str(t.qualified_name) for t in plan.ordered_tables} == {"cat.sch.employees"}


def test_resolve_builds_skipped_names_by_table_index():
    # Given orders has an unresolvable FK
    tables = (_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When
    plan = resolve(tables)

    # Then the index has the constraint name for orders
    assert "cat.sch.orders" in plan.skipped_names_by_table
    assert "orders_ref_id_fk" in plan.skipped_names_by_table["cat.sch.orders"]


def test_resolve_with_empty_tables_returns_empty_plan():
    # Given / When
    plan = resolve(())

    # Then
    assert plan.ordered_tables == ()
    assert plan.skipped_foreign_keys == ()
    assert plan.skipped_names_by_table == {}
