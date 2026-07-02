"""
Unit tests for dependency_resolution.resolve().

These exercise resolve() directly: dependency-first ordering, cycle detection,
self-reference handling, and the per-table fail-closed verdict
(CYCLE / UNRESOLVABLE_REFERENCE / BLOCKED_BY_FAILED_DEPENDENCY), including
transitive propagation to dependents.
"""

from delta_engine.api import Column, DeltaTable, String
from delta_engine.application.dependency_resolution import ResolveResult, resolve
from delta_engine.application.results import ForeignKeyFailureReason
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.table import DesiredTable


def _table(fqn: str) -> DesiredTable:
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog,
        schema,
        name,
        columns=(Column("id", String(), nullable=False, primary_key=True),),
    ).to_desired_table()


def _table_with_fk(fqn: str, references: str) -> DesiredTable:
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog,
        schema,
        name,
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("ref_id", String()),
        ),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("ref_id",),
                references=references,
                referenced_columns=("id",),
            )
        ],
    ).to_desired_table()


def _names(result: ResolveResult) -> list[str]:
    """Ordered table names, as strings, in dependency-first order."""
    return [str(name) for name in result.ordered_names]


def _failures_for(result: ResolveResult, fqn: str) -> tuple:
    """FK failures resolve recorded for one table (empty tuple if none)."""
    for name, failures in result.fk_failures.items():
        if str(name) == fqn:
            return failures
    return ()


def test_resolve_with_no_fks_preserves_registry_order():
    # Given three tables with no FKs
    tables = (_table("cat.sch.a"), _table("cat.sch.b"), _table("cat.sch.c"))

    # When
    result = resolve(tables)

    # Then order is unchanged and all tables have no FK failures
    assert _names(result) == ["cat.sch.a", "cat.sch.b", "cat.sch.c"]
    assert not result.fk_failures


def test_resolve_orders_referenced_table_before_dependent():
    # Given orders depends on customers
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When
    result = resolve(tables)

    # Then customers appears before orders and all tables have no FK failures
    names = _names(result)
    assert names.index("cat.sch.customers") < names.index("cat.sch.orders")
    assert not result.fk_failures


def test_resolve_handles_chain_of_dependencies():
    # Given c -> b -> a (a must sync first, then b, then c)
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table("cat.sch.a"),
    )

    # When
    result = resolve(tables)

    # Then a before b before c
    names = _names(result)
    assert names.index("cat.sch.a") < names.index("cat.sch.b") < names.index("cat.sch.c")


def test_resolve_fails_table_with_unresolvable_reference():
    # Given orders references customers but customers is not registered
    tables = (_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When
    result = resolve(tables)

    # Then orders has one UNRESOLVABLE_REFERENCE failure with the FK's columns
    failures = _failures_for(result, "cat.sch.orders")
    assert len(failures) == 1
    assert failures[0].reason == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    assert failures[0].local_columns == ("ref_id",)
    assert failures[0].references == "cat.sch.customers"


def test_resolve_fails_both_members_of_a_cycle():
    # Given a -> b and b -> a (mutual cycle)
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
    )

    # When
    result = resolve(tables)

    # Then both tables cannot execute, each with CYCLE
    assert _failures_for(result, "cat.sch.a")
    assert _failures_for(result, "cat.sch.b")
    assert _failures_for(result, "cat.sch.a")[0].reason == ForeignKeyFailureReason.CYCLE
    assert _failures_for(result, "cat.sch.b")[0].reason == ForeignKeyFailureReason.CYCLE


def test_resolve_ordering_includes_failed_tables():
    # Given a mutual cycle between a and b
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
    )

    # When
    result = resolve(tables)

    # Then both tables still appear in the ordering (the engine gates them out via their failures)
    assert set(_names(result)) == {"cat.sch.a", "cat.sch.b"}


def test_resolve_blocks_table_that_references_an_unresolvable_table():
    # Given orders -> customers and customers -> archive (archive not registered)
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table_with_fk("cat.sch.customers", "cat.sch.archive"),
    )

    # When
    result = resolve(tables)

    # Then customers fails directly, and orders cannot execute because customers will not build
    assert (
        _failures_for(result, "cat.sch.customers")[0].reason
        == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    assert (
        _failures_for(result, "cat.sch.orders")[0].reason
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
    result = resolve(tables)

    # Then a fails directly and b, c, d are all blocked transitively
    assert (
        _failures_for(result, "cat.sch.a")[0].reason
        == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    for blocked in ("cat.sch.b", "cat.sch.c", "cat.sch.d"):
        assert (
            _failures_for(result, blocked)[0].reason
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
    result = resolve(tables)

    # Then b and c fail as CYCLE, and a cannot execute because b will not build
    assert _failures_for(result, "cat.sch.b")[0].reason == ForeignKeyFailureReason.CYCLE
    assert _failures_for(result, "cat.sch.c")[0].reason == ForeignKeyFailureReason.CYCLE
    assert (
        _failures_for(result, "cat.sch.a")[0].reason
        == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    )


def test_resolve_does_not_block_an_unrelated_sibling():
    # Given orders -> missing (fails), and an unrelated table with no FKs
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.missing"),
        _table("cat.sch.unrelated"),
    )

    # When
    result = resolve(tables)

    # Then only orders cannot execute; the unrelated table is fine
    assert _failures_for(result, "cat.sch.orders")
    assert _failures_for(result, "cat.sch.unrelated") == ()


def test_resolve_treats_self_referential_fk_as_applicable():
    # Given a table whose foreign key references itself (a self-loop)
    table = DeltaTable(
        "cat",
        "sch",
        "employees",
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("manager_id", String()),
        ),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("manager_id",),
                references="cat.sch.employees",
                referenced_columns=("id",),
            )
        ],
    ).to_desired_table()

    # When
    result = resolve((table,))

    # Then the self-referencing FK does not prevent execution
    assert _names(result) == ["cat.sch.employees"]
    assert _failures_for(result, "cat.sch.employees") == ()


def test_resolve_propagates_block_through_a_diamond():
    # Given a diamond: d depends on b and c; both b and c depend on a;
    # a references an unregistered table
    table_d = DeltaTable(
        "cat",
        "sch",
        "d",
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("b_id", String()),
            Column("c_id", String()),
        ),
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
    result = resolve(tables)

    # Then a fails directly; b, c, and d are all blocked
    assert (
        _failures_for(result, "cat.sch.a")[0].reason
        == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    for blocked in ("cat.sch.b", "cat.sch.c", "cat.sch.d"):
        assert all(
            f.reason == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
            for f in _failures_for(result, blocked)
        )
    # d has two blocking FKs, so it records two failures (one per FK)
    assert len(_failures_for(result, "cat.sch.d")) == 2


def test_resolve_with_empty_tables_returns_empty_tuple():
    # Given / When
    result = resolve(())

    # Then
    assert result.ordered_names == () and result.fk_failures == {}


def test_resolve_blocks_table_passed_in_blocked_set():
    # Given one table with no FKs, named as already-blocked (e.g. failed validation upstream)
    table = _table("cat.sch.orders")
    blocked = frozenset({QualifiedName("cat", "sch", "orders")})

    # When resolving with that table blocked
    result = resolve((table,), blocked=blocked)

    # Then it produces no FK failures of its own (it was blocked for an external reason)
    # and, having no FKs, it is not further classified by resolve
    assert _failures_for(result, "cat.sch.orders") == ()


def test_resolve_blocks_fk_dependent_of_a_blocked_table():
    # Given orders (no FKs) is blocked, and shipments has a FK on orders
    orders = _table("cat.sch.orders")
    shipments = _table_with_fk("cat.sch.shipments", "cat.sch.orders")
    blocked = frozenset({QualifiedName("cat", "sch", "orders")})

    # When resolving
    result = resolve((orders, shipments), blocked=blocked)

    # Then shipments is blocked-by-failed-dependency; orders itself has no FK failure
    assert _failures_for(result, "cat.sch.orders") == ()
    assert _failures_for(result, "cat.sch.shipments")
    assert (
        _failures_for(result, "cat.sch.shipments")[0].reason
        == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    )


def test_resolve_passes_when_fk_targets_the_parents_primary_key():
    # Given orders.ref_id -> customers.id, and customers declares id as its PK
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When
    result = resolve(tables)

    # Then every table can execute
    assert not result.fk_failures


def test_resolve_fails_fk_that_targets_a_non_key_column():
    # Given orders.ref_id -> customers.id, but customers has NO primary key
    customers_no_pk = DeltaTable(
        "cat",
        "sch",
        "customers",
        columns=(Column("id", String()),),
    ).to_desired_table()
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        customers_no_pk,
    )

    # When
    result = resolve(tables)

    # Then orders cannot execute: its referenced columns are not the parent's PK
    assert _failures_for(result, "cat.sch.orders")
    assert (
        _failures_for(result, "cat.sch.orders")[0].reason
        == ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY
    )
    # customers has no FK of its own, so it is unaffected and can execute
    assert _failures_for(result, "cat.sch.customers") == ()


def test_resolve_fails_fk_whose_referenced_columns_are_not_the_pk():
    # Given customers' PK is (id) but orders references customers(email)
    catalog, schema, name = "cat.sch.customers".split(".")
    customers = DeltaTable(
        catalog,
        schema,
        name,
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("email", String()),
        ),
    ).to_desired_table()
    orders = DeltaTable(
        "cat",
        "sch",
        "orders",
        columns=(Column("id", String()), Column("ref_email", String())),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("ref_email",),
                references="cat.sch.customers",
                referenced_columns=("email",),
            )
        ],
    ).to_desired_table()

    # When
    result = resolve((orders, customers))

    # Then orders is rejected: email is not customers' primary key
    assert (
        _failures_for(result, "cat.sch.orders")[0].reason
        == ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY
    )


def test_resolve_valid_chain_with_primary_keys_executes():
    # Given c.ref_id -> a.id, a.ref_id -> b.id, and both a and b expose id as PK.
    # a must therefore be a table that has BOTH a PK (id) and an FK (ref_id -> b).
    a = DeltaTable(
        "cat",
        "sch",
        "a",
        columns=(
            Column("id", String(), nullable=False, primary_key=True),
            Column("ref_id", String()),
        ),
        foreign_keys=[
            ForeignKeyConstraint(
                local_columns=("ref_id",), references="cat.sch.b", referenced_columns=("id",)
            )
        ],
    ).to_desired_table()
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.a"),
        a,
        _table("cat.sch.b"),
    )

    # When
    result = resolve(tables)

    # Then the whole chain validates and executes
    assert not result.fk_failures
