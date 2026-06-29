"""
Unit tests for foreign_key_planning.resolve().

These exercise resolve() directly: dependency-first ordering, cycle detection,
self-reference handling, and the per-table fail-closed verdict
(CYCLE / UNRESOLVABLE_REFERENCE / BLOCKED_BY_FAILED_DEPENDENCY), including
transitive propagation to dependents.
"""

from delta_engine.api import Column, DeltaTable, String
from delta_engine.application.foreign_key_planning import resolve
from delta_engine.application.results import ForeignKeyFailureReason
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.model.table import DesiredTable


def _qn(fqn: str) -> QualifiedName:
    catalog, schema, name = fqn.split(".")
    return QualifiedName(catalog, schema, name)


def _table(fqn: str) -> DesiredTable:
    catalog, schema, name = fqn.split(".")
    return DeltaTable(catalog, schema, name, columns=(Column("id", String()),)).to_desired_table()


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
    tables = (_table("cat.sch.a"), _table("cat.sch.b"), _table("cat.sch.c"))

    # When
    resolution = resolve(tables)

    # Then order is unchanged and nothing fails
    names = [str(table.qualified_name) for table in resolution.ordered_tables]
    assert names == ["cat.sch.a", "cat.sch.b", "cat.sch.c"]
    assert resolution.failures_by_table == {}


def test_resolve_orders_referenced_table_before_dependent():
    # Given orders depends on customers
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When
    resolution = resolve(tables)

    # Then customers appears before orders and neither fails
    names = [str(table.qualified_name) for table in resolution.ordered_tables]
    assert names.index("cat.sch.customers") < names.index("cat.sch.orders")
    assert not resolution.fails(_qn("cat.sch.orders"))


def test_resolve_handles_chain_of_dependencies():
    # Given c -> b -> a (a must sync first, then b, then c)
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table("cat.sch.a"),
    )

    # When
    resolution = resolve(tables)

    # Then a before b before c
    names = [str(table.qualified_name) for table in resolution.ordered_tables]
    assert names.index("cat.sch.a") < names.index("cat.sch.b") < names.index("cat.sch.c")


def test_resolve_fails_table_with_unresolvable_reference():
    # Given orders references customers but customers is not registered
    tables = (_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When
    resolution = resolve(tables)

    # Then orders fails with UNRESOLVABLE_REFERENCE
    failures = resolution.failures_for(_qn("cat.sch.orders"))
    assert len(failures) == 1
    assert failures[0].reason == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    assert failures[0].constraint_name == "orders_ref_id_fk"


def test_resolve_fails_both_members_of_a_cycle():
    # Given a -> b and b -> a (mutual cycle)
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
    )

    # When
    resolution = resolve(tables)

    # Then both tables fail with CYCLE
    assert resolution.fails(_qn("cat.sch.a"))
    assert resolution.fails(_qn("cat.sch.b"))
    assert resolution.failures_for(_qn("cat.sch.a"))[0].reason == ForeignKeyFailureReason.CYCLE
    assert resolution.failures_for(_qn("cat.sch.b"))[0].reason == ForeignKeyFailureReason.CYCLE


def test_resolve_includes_failed_tables_in_ordered_tables():
    # Given a mutual cycle between a and b
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
    )

    # When
    resolution = resolve(tables)

    # Then both tables still appear in ordered_tables (the engine gates them out)
    names = {str(table.qualified_name) for table in resolution.ordered_tables}
    assert names == {"cat.sch.a", "cat.sch.b"}


def test_resolve_blocks_table_that_references_an_unresolvable_table():
    # Given orders -> customers and customers -> archive (archive not registered)
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table_with_fk("cat.sch.customers", "cat.sch.archive"),
    )

    # When
    resolution = resolve(tables)

    # Then customers fails directly, and orders is blocked because customers won't build
    assert (
        resolution.failures_for(_qn("cat.sch.customers"))[0].reason
        == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    assert (
        resolution.failures_for(_qn("cat.sch.orders"))[0].reason
        == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    )


def test_resolve_propagates_block_along_a_chain():
    # Given d -> c -> b -> a, where a references an unregistered table
    tables = (
        _table_with_fk("cat.sch.d", "cat.sch.c"),
        _table_with_fk("cat.sch.c", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table_with_fk("cat.sch.a", "cat.sch.missing"),
    )

    # When
    resolution = resolve(tables)

    # Then a fails directly and b, c, d are all blocked transitively
    assert (
        resolution.failures_for(_qn("cat.sch.a"))[0].reason
        == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    for blocked in ("cat.sch.b", "cat.sch.c", "cat.sch.d"):
        assert (
            resolution.failures_for(_qn(blocked))[0].reason
            == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
        )


def test_resolve_blocks_table_that_depends_on_a_cycle():
    # Given b <-> c form a mutual cycle, and a depends on b
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.c"),
        _table_with_fk("cat.sch.c", "cat.sch.b"),
    )

    # When
    resolution = resolve(tables)

    # Then b and c fail as CYCLE, and a is blocked (its dependency b won't build)
    assert resolution.failures_for(_qn("cat.sch.b"))[0].reason == ForeignKeyFailureReason.CYCLE
    assert resolution.failures_for(_qn("cat.sch.c"))[0].reason == ForeignKeyFailureReason.CYCLE
    assert (
        resolution.failures_for(_qn("cat.sch.a"))[0].reason
        == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    )


def test_resolve_does_not_block_an_unrelated_sibling():
    # Given orders -> missing (fails), and an unrelated table with no FKs
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.missing"),
        _table("cat.sch.unrelated"),
    )

    # When
    resolution = resolve(tables)

    # Then only orders fails; the unrelated table is fine
    assert resolution.fails(_qn("cat.sch.orders"))
    assert not resolution.fails(_qn("cat.sch.unrelated"))


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
    resolution = resolve((table,))

    # Then the self-referencing FK does not fail the table
    assert not resolution.fails(_qn("cat.sch.employees"))
    assert {str(t.qualified_name) for t in resolution.ordered_tables} == {"cat.sch.employees"}


def test_resolve_propagates_block_through_a_diamond():
    # Given a diamond: d depends on b and c; both b and c depend on a;
    # a references an unregistered table
    table_d = DeltaTable(
        "cat",
        "sch",
        "d",
        columns=(Column("id", String()), Column("b_id", String()), Column("c_id", String())),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("b_id",), references="cat.sch.b", referenced_columns=("id",)
            ),
            ForeignKeyConstraint(
                local_columns=("c_id",), references="cat.sch.c", referenced_columns=("id",)
            ),
        ],
    ).to_desired_table()
    tables = (
        table_d,
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table_with_fk("cat.sch.c", "cat.sch.a"),
        _table_with_fk("cat.sch.a", "cat.sch.missing"),
    )

    # When
    resolution = resolve(tables)

    # Then a fails directly; b, c, and the fan-in node d are all blocked exactly once
    assert (
        resolution.failures_for(_qn("cat.sch.a"))[0].reason
        == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    for blocked in ("cat.sch.b", "cat.sch.c", "cat.sch.d"):
        failures = resolution.failures_for(_qn(blocked))
        assert all(
            failure.reason == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
            for failure in failures
        )
    # And d is recorded once per blocking FK (two), with no duplicate entries beyond that
    assert len(resolution.failures_for(_qn("cat.sch.d"))) == 2


def test_resolve_with_empty_tables_returns_empty_resolution():
    # Given / When
    resolution = resolve(())

    # Then
    assert resolution.ordered_tables == ()
    assert resolution.failures_by_table == {}
