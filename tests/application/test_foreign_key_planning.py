"""
Unit tests for foreign_key_planning.resolve().

These exercise resolve() directly: dependency-first ordering, cycle detection,
self-reference handling, and the per-table fail-closed verdict
(CYCLE / UNRESOLVABLE_REFERENCE / BLOCKED_BY_FAILED_DEPENDENCY), including
transitive propagation to dependents.
"""

from delta_engine.api import Column, DeltaTable, String
from delta_engine.application.foreign_key_planning import SyncCandidate, resolve
from delta_engine.application.results import ForeignKeyFailureReason, ValidationFailure
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.table import DesiredTable


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


def _candidates_by_name(
    candidates: tuple[SyncCandidate, ...],
) -> dict[str, SyncCandidate]:
    return {str(c.table.qualified_name): c for c in candidates}


def test_resolve_with_no_fks_preserves_registry_order():
    # Given three tables with no FKs
    tables = (_table("cat.sch.a"), _table("cat.sch.b"), _table("cat.sch.c"))

    # When
    candidates = resolve(tables)

    # Then order is unchanged and all candidates can execute
    names = [str(candidate.table.qualified_name) for candidate in candidates]
    assert names == ["cat.sch.a", "cat.sch.b", "cat.sch.c"]
    assert all(candidate.can_execute for candidate in candidates)


def test_resolve_orders_referenced_table_before_dependent():
    # Given orders depends on customers
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When
    candidates = resolve(tables)

    # Then customers appears before orders and all candidates can execute
    names = [str(candidate.table.qualified_name) for candidate in candidates]
    assert names.index("cat.sch.customers") < names.index("cat.sch.orders")
    assert all(candidate.can_execute for candidate in candidates)


def test_resolve_handles_chain_of_dependencies():
    # Given c -> b -> a (a must sync first, then b, then c)
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table("cat.sch.a"),
    )

    # When
    candidates = resolve(tables)

    # Then a before b before c
    names = [str(candidate.table.qualified_name) for candidate in candidates]
    assert names.index("cat.sch.a") < names.index("cat.sch.b") < names.index("cat.sch.c")


def test_resolve_fails_table_with_unresolvable_reference():
    # Given orders references customers but customers is not registered
    tables = (_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When
    candidates = resolve(tables)

    # Then orders cannot execute, with UNRESOLVABLE_REFERENCE
    [candidate] = candidates
    assert not candidate.can_execute
    assert len(candidate.failures) == 1
    assert candidate.failures[0].reason == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    assert candidate.failures[0].constraint_name == "orders_ref_id_fk"


def test_resolve_fails_both_members_of_a_cycle():
    # Given a -> b and b -> a (mutual cycle)
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
    )

    # When
    candidates = resolve(tables)

    # Then both tables cannot execute, each with CYCLE
    by_name = _candidates_by_name(candidates)
    assert not by_name["cat.sch.a"].can_execute
    assert not by_name["cat.sch.b"].can_execute
    assert by_name["cat.sch.a"].failures[0].reason == ForeignKeyFailureReason.CYCLE
    assert by_name["cat.sch.b"].failures[0].reason == ForeignKeyFailureReason.CYCLE


def test_resolve_includes_failed_tables_in_candidates():
    # Given a mutual cycle between a and b
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
    )

    # When
    candidates = resolve(tables)

    # Then both tables still appear as candidates (the engine gates them out via can_execute)
    names = {str(candidate.table.qualified_name) for candidate in candidates}
    assert names == {"cat.sch.a", "cat.sch.b"}


def test_resolve_blocks_table_that_references_an_unresolvable_table():
    # Given orders -> customers and customers -> archive (archive not registered)
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table_with_fk("cat.sch.customers", "cat.sch.archive"),
    )

    # When
    candidates = resolve(tables)

    # Then customers fails directly, and orders cannot execute because customers will not build
    by_name = _candidates_by_name(candidates)
    assert (
        by_name["cat.sch.customers"].failures[0].reason
        == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    assert (
        by_name["cat.sch.orders"].failures[0].reason
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
    candidates = resolve(tables)

    # Then a fails directly and b, c, d are all blocked transitively
    by_name = _candidates_by_name(candidates)
    assert (
        by_name["cat.sch.a"].failures[0].reason == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    for blocked in ("cat.sch.b", "cat.sch.c", "cat.sch.d"):
        assert (
            by_name[blocked].failures[0].reason
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
    candidates = resolve(tables)

    # Then b and c fail as CYCLE, and a cannot execute because b will not build
    by_name = _candidates_by_name(candidates)
    assert by_name["cat.sch.b"].failures[0].reason == ForeignKeyFailureReason.CYCLE
    assert by_name["cat.sch.c"].failures[0].reason == ForeignKeyFailureReason.CYCLE
    assert (
        by_name["cat.sch.a"].failures[0].reason
        == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    )


def test_resolve_does_not_block_an_unrelated_sibling():
    # Given orders -> missing (fails), and an unrelated table with no FKs
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.missing"),
        _table("cat.sch.unrelated"),
    )

    # When
    candidates = resolve(tables)

    # Then only orders cannot execute; the unrelated table is fine
    by_name = _candidates_by_name(candidates)
    assert not by_name["cat.sch.orders"].can_execute
    assert by_name["cat.sch.unrelated"].can_execute


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
    candidates = resolve((table,))

    # Then the self-referencing FK does not prevent execution
    [candidate] = candidates
    assert candidate.can_execute
    assert str(candidate.table.qualified_name) == "cat.sch.employees"


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
    candidates = resolve(tables)

    # Then a fails directly; b, c, and d are all blocked
    by_name = _candidates_by_name(candidates)
    assert (
        by_name["cat.sch.a"].failures[0].reason == ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )
    for blocked in ("cat.sch.b", "cat.sch.c", "cat.sch.d"):
        assert all(
            f.reason == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
            for f in by_name[blocked].failures
        )
    # d has two blocking FKs, so it records two failures (one per FK)
    assert len(by_name["cat.sch.d"].failures) == 2


def test_resolve_with_empty_tables_returns_empty_tuple():
    # Given / When
    candidates = resolve(())

    # Then
    assert candidates == ()


def test_resolve_blocks_table_passed_in_external_failures():
    # Given one table with no FKs, passed as externally failed
    table = _table("cat.sch.orders")
    external_failures = {
        QualifiedName("cat", "sch", "orders"): (
            ValidationFailure(rule_name="NonNullableColumnAdd", message="cannot add NOT NULL"),
        )
    }

    # When
    candidates = resolve((table,), external_failures=external_failures)

    # Then orders cannot execute and carries the validation failure
    [candidate] = candidates
    assert not candidate.can_execute
    assert len(candidate.failures) == 1
    assert isinstance(candidate.failures[0], ValidationFailure)


def test_resolve_blocks_fk_dependent_of_externally_failed_table():
    # Given orders (no FKs) is externally failed, and shipments has a FK on orders
    orders = _table("cat.sch.orders")
    shipments = _table_with_fk("cat.sch.shipments", "cat.sch.orders")
    external_failures = {
        QualifiedName("cat", "sch", "orders"): (
            ValidationFailure(rule_name="NonNullableColumnAdd", message="cannot add NOT NULL"),
        )
    }

    # When
    candidates = resolve((orders, shipments), external_failures=external_failures)

    # Then orders carries the validation failure; shipments is blocked by failed dependency
    by_name = _candidates_by_name(candidates)
    assert not by_name["cat.sch.orders"].can_execute
    assert not by_name["cat.sch.shipments"].can_execute
    assert (
        by_name["cat.sch.shipments"].failures[0].reason
        == ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    )
