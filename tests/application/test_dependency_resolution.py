"""
Unit tests for dependency_resolution.resolve().

These tests exercise the public resolver API rather than the graph traversal
implementation details. They cover dependency-first ordering, direct FK failure
classification, cycle detection, self-reference handling, and propagation from
failed dependencies.
"""

from delta_engine.application.dependency_resolution import ResolveResult, resolve
from delta_engine.application.failures import ForeignKeyFailureReason
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
from delta_engine.domain.model.table import DesiredTable
from delta_engine.schema import Column, DeltaTable, ForeignKey, Self, String


def _qualified_name(fqn: str) -> QualifiedName:
    return QualifiedName.parse(fqn)


def _split_fqn(fqn: str) -> tuple[str, str, str]:
    catalog, schema, table_name = fqn.split(".")
    return catalog, schema, table_name


def _referenced_table(
    fqn: str,
    *,
    primary_key_columns: tuple[str, ...] = ("id",),
) -> DeltaTable:
    """Build a minimal public API table for use as an FK target."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=tuple(
            Column(
                column_name,
                String(),
                nullable=False,
            )
            for column_name in primary_key_columns
        ),
        primary_key=list(primary_key_columns) if primary_key_columns else None,
    )


def _table(
    fqn: str,
    *,
    primary_key_columns: tuple[str, ...] = ("id",),
    extra_columns: tuple[str, ...] = (),
) -> DesiredTable:
    catalog, schema, table_name = _split_fqn(fqn)

    primary_key_column_definitions = tuple(
        Column(
            column_name,
            String(),
            nullable=False,
        )
        for column_name in primary_key_columns
    )
    extra_column_definitions = tuple(Column(column_name, String()) for column_name in extra_columns)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=primary_key_column_definitions + extra_column_definitions,
        primary_key=list(primary_key_columns) if primary_key_columns else None,
    ).to_desired_table()


def _table_with_fk(
    fqn: str,
    references: str,
    *,
    local_columns: tuple[str, ...] = ("ref_id",),
    referenced_primary_key_columns: tuple[str, ...] = ("id",),
) -> DesiredTable:
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(
            Column("id", String(), nullable=False),
            *tuple(Column(column_name, String()) for column_name in local_columns),
        ),
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(
                columns=dict(zip(local_columns, referenced_primary_key_columns, strict=True)),
                references=_referenced_table(
                    references,
                    primary_key_columns=referenced_primary_key_columns,
                ),
            )
        ],
    ).to_desired_table()


def _fk_column_name(referenced_fqn: str) -> str:
    """Derive the local FK column name for a reference: cat.sch.orders -> orders_id."""
    _, _, referenced_table_name = _split_fqn(referenced_fqn)
    return f"{referenced_table_name}_id"


def _table_with_fks(fqn: str, *references: str) -> DesiredTable:
    """
    Build a table with one single-column foreign key per referenced table.

    Each foreign key's local column is named after its referenced table:
    a reference to cat.sch.orders uses local column orders_id.
    """
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(
            Column("id", String(), nullable=False),
            *(Column(_fk_column_name(reference), String()) for reference in references),
        ),
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(
                columns={_fk_column_name(reference): "id"},
                references=_referenced_table(reference),
            )
            for reference in references
        ],
    ).to_desired_table()


def _tag_scoped_table_with_fk(fqn: str, references: str) -> DesiredTable:
    """Build a tag-scoped table that carries an FK it does not manage."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(
            Column("id", String(), nullable=False),
            Column("ref_id", String()),
        ),
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(
                columns={"ref_id": "id"},
                references=_referenced_table(references),
            )
        ],
        scope="tags",
    ).to_desired_table()


def _names(result: ResolveResult) -> list[str]:
    return [str(name) for name in result.ordered_names]


def _failures_for(result: ResolveResult, fqn: str) -> tuple:
    return result.fk_failures.get(_qualified_name(fqn), ())


def _failure_reasons_for(
    result: ResolveResult,
    fqn: str,
) -> list[ForeignKeyFailureReason]:
    return [failure.reason for failure in _failures_for(result, fqn)]


def _assert_before(result: ResolveResult, parent: str, child: str) -> None:
    names = _names(result)
    assert names.index(parent) < names.index(child)


def _assert_has_failure(
    result: ResolveResult,
    fqn: str,
    *,
    reason: ForeignKeyFailureReason,
    references: str | None = None,
    local_columns: tuple[str, ...] | None = None,
) -> None:
    failures = _failures_for(result, fqn)

    matching_failures = [failure for failure in failures if failure.reason == reason]

    if references is not None:
        matching_failures = [
            failure
            for failure in matching_failures
            if failure.references == _qualified_name(references)
        ]

    if local_columns is not None:
        matching_failures = [
            failure for failure in matching_failures if failure.local_columns == local_columns
        ]

    assert matching_failures


def test_resolve_with_empty_tables_returns_empty_result():
    # Given no registered tables
    # When resolving dependencies
    result = resolve(())

    # Then the result is empty
    assert result.ordered_names == ()
    assert result.fk_failures == {}


def test_resolve_with_no_fks_preserves_prepared_input_order():
    # Given three independent tables in prepared order
    tables = (
        _table("cat.sch.a"),
        _table("cat.sch.b"),
        _table("cat.sch.c"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then the independent tables stay in prepared input order
    assert _names(result) == ["cat.sch.a", "cat.sch.b", "cat.sch.c"]
    assert not result.fk_failures


def test_resolve_ignores_foreign_keys_on_tag_scoped_declarations():
    # Given a tag-scoped declaration that carries an FK but does not manage FKs
    tables = (_tag_scoped_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When resolving dependencies
    result = resolve(tables)

    # Then the carried FK does not produce an unresolvable-reference failure
    assert _names(result) == ["cat.sch.orders"]
    assert not result.fk_failures


def test_resolve_does_not_order_by_unmanaged_foreign_keys():
    # Given a tag-scoped declaration listed before the table it references
    tables = (
        _tag_scoped_table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then the unmanaged FK does not impose parent-before-child ordering
    assert _names(result) == ["cat.sch.orders", "cat.sch.customers"]
    assert not result.fk_failures


def test_resolve_orders_referenced_table_before_dependent():
    # Given orders depends on customers
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then customers appears before orders
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not result.fk_failures


def test_resolve_orders_referenced_tag_scoped_table_before_dependent():
    # Given a managed table whose foreign key targets a tag-scoped table
    customers = DeltaTable(
        "cat",
        "sch",
        "customers",
        columns=(Column("id", String(), nullable=False),),
        primary_key=["id"],
        scope="tags",
    )
    orders = DeltaTable(
        "cat",
        "sch",
        "orders",
        columns=(
            Column("id", String(), nullable=False),
            Column("customer_id", String()),
        ),
        primary_key=["id"],
        foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
    ).to_desired_table()
    tables = (orders, customers.to_desired_table())

    # When resolving dependencies
    result = resolve(tables)

    # Then the referenced tag-scoped table is ordered before its managed dependent
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not result.fk_failures


def test_resolve_handles_chain_of_dependencies():
    # Given c -> b -> a
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table("cat.sch.a"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then dependencies are ordered before their dependents
    _assert_before(result, "cat.sch.a", "cat.sch.b")
    _assert_before(result, "cat.sch.b", "cat.sch.c")
    assert not result.fk_failures


def test_resolve_orders_table_after_all_fk_parents():
    # Given order_items depends on both orders and products
    tables = (
        _table_with_fks("cat.sch.order_items", "cat.sch.orders", "cat.sch.products"),
        _table("cat.sch.products"),
        _table("cat.sch.orders"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then both parent tables appear before the dependent table
    _assert_before(result, "cat.sch.orders", "cat.sch.order_items")
    _assert_before(result, "cat.sch.products", "cat.sch.order_items")
    assert not result.fk_failures


def test_resolve_fails_table_with_unresolvable_reference():
    # Given orders references customers but customers is not registered
    tables = (_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When resolving dependencies
    result = resolve(tables)

    # Then orders has one unresolvable-reference failure with the FK details
    failures = _failures_for(result, "cat.sch.orders")

    assert len(failures) == 1
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        references="cat.sch.customers",
        local_columns=("ref_id",),
    )


def test_resolve_records_one_failure_per_unresolvable_fk():
    # Given a table has two FKs to missing tables
    shipments = _table_with_fks("cat.sch.shipments", "cat.sch.orders", "cat.sch.customers")

    # When resolving dependencies
    result = resolve((shipments,))

    # Then both failed FK relationships are reported
    failures = _failures_for(result, "cat.sch.shipments")

    assert {
        (failure.local_columns, failure.references, failure.reason) for failure in failures
    } == {
        (
            ("orders_id",),
            _qualified_name("cat.sch.orders"),
            ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        ),
        (
            ("customers_id",),
            _qualified_name("cat.sch.customers"),
            ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        ),
    }


def test_resolve_fails_both_members_of_a_mutual_cycle_and_still_orders_them():
    # Given a -> b and b -> a
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then both cycle members fail and both still appear in the ordering
    _assert_has_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.b",
    )
    _assert_has_failure(
        result,
        "cat.sch.b",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.a",
    )
    assert set(_names(result)) == {"cat.sch.a", "cat.sch.b"}


def test_resolve_fails_all_members_of_three_table_cycle():
    # Given a -> b -> c -> a
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.c"),
        _table_with_fk("cat.sch.c", "cat.sch.a"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then every cycle member fails with a cycle failure
    for table_name in ("cat.sch.a", "cat.sch.b", "cat.sch.c"):
        _assert_has_failure(
            result,
            table_name,
            reason=ForeignKeyFailureReason.CYCLE,
        )

    assert set(_names(result)) == {"cat.sch.a", "cat.sch.b", "cat.sch.c"}


def test_resolve_blocks_table_that_references_an_unresolvable_table():
    # Given orders -> customers and customers -> archive, where archive is missing
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table_with_fk("cat.sch.customers", "cat.sch.archive"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then customers fails directly and orders is blocked by customers
    _assert_has_failure(
        result,
        "cat.sch.customers",
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        references="cat.sch.archive",
    )
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.customers",
    )


def test_resolve_propagates_block_along_a_chain():
    # Given d -> c -> b -> a, where a references a missing table
    tables = (
        _table_with_fk("cat.sch.d", "cat.sch.c"),
        _table_with_fk("cat.sch.c", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table_with_fk("cat.sch.a", "cat.sch.missing"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then a fails directly and the block propagates to b, c, and d
    _assert_has_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        references="cat.sch.missing",
    )

    for dependent, failed_parent in (
        ("cat.sch.b", "cat.sch.a"),
        ("cat.sch.c", "cat.sch.b"),
        ("cat.sch.d", "cat.sch.c"),
    ):
        _assert_has_failure(
            result,
            dependent,
            reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
            references=failed_parent,
        )


def test_resolve_records_cycle_failure_only_for_fk_inside_the_cycle():
    # Given a <-> b form a cycle, and a also references healthy table c
    tables = (
        _table_with_fks("cat.sch.a", "cat.sch.b", "cat.sch.c"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table("cat.sch.c"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then a's FK into the cycle fails, but its FK to the healthy table does not
    failures_for_a = _failures_for(result, "cat.sch.a")

    assert len(failures_for_a) == 1
    _assert_has_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.b",
    )
    _assert_has_failure(
        result,
        "cat.sch.b",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.a",
    )
    assert _failures_for(result, "cat.sch.c") == ()


def test_resolve_reports_invalid_fk_target_over_cycle_for_the_same_fk():
    # Given a <-> b form a cycle, but a's FK into b targets a non-primary-key column.
    # The FK-target check runs before cycle classification (see _classify_failures),
    # so the structural problem is reported per-FK even though a is also in a cycle.
    b = DesiredTable(
        qualified_name=_qualified_name("cat.sch.b"),
        columns=(
            Column("id", String(), nullable=False),
            Column("email", String()),
            Column("ref_id", String()),
        ),
        primary_key=PrimaryKeyConstraint.generate(table_name="b", columns=("id",)),
        foreign_keys=(
            ForeignKeyConstraint.generate(
                owner_table_name="b",
                local_columns=("ref_id",),
                referenced_table=_qualified_name("cat.sch.a"),
                referenced_columns=("id",),
            ),
        ),
    )
    a = DesiredTable(
        qualified_name=_qualified_name("cat.sch.a"),
        columns=(
            Column("id", String(), nullable=False),
            Column("ref_email", String()),
        ),
        primary_key=PrimaryKeyConstraint.generate(table_name="a", columns=("id",)),
        foreign_keys=(
            ForeignKeyConstraint.generate(
                owner_table_name="a",
                local_columns=("ref_email",),
                referenced_table=_qualified_name("cat.sch.b"),
                referenced_columns=("email",),
            ),
        ),
    )

    # When resolving dependencies
    result = resolve((a, b))

    # Then a's single failure is the invalid FK target, not a cycle failure
    _assert_has_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.b",
        local_columns=("ref_email",),
    )
    assert _failure_reasons_for(result, "cat.sch.a") == [
        ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY
    ]


def test_resolve_blocks_table_that_depends_on_a_cycle():
    # Given b <-> c form a cycle, and a depends on b
    tables = (
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.c"),
        _table_with_fk("cat.sch.c", "cat.sch.b"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then b and c fail directly, and a is blocked by b
    _assert_has_failure(
        result,
        "cat.sch.b",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.c",
    )
    _assert_has_failure(
        result,
        "cat.sch.c",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.b",
    )
    _assert_has_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.b",
    )


def test_resolve_does_not_block_an_unrelated_sibling():
    # Given orders references a missing table and unrelated has no FKs
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.missing"),
        _table("cat.sch.unrelated"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then only orders fails
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        references="cat.sch.missing",
    )
    assert _failures_for(result, "cat.sch.unrelated") == ()


def test_resolve_treats_self_referential_fk_as_applicable():
    # Given employees has a self-referential manager FK
    employees = DeltaTable(
        "cat",
        "sch",
        "employees",
        columns=(
            Column("id", String(), nullable=False),
            Column("manager_id", String()),
        ),
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(
                columns={"manager_id": "id"},
                references=Self,
            )
        ],
    ).to_desired_table()

    # When resolving dependencies
    result = resolve((employees,))

    # Then the self-reference does not prevent the table from executing
    assert _names(result) == ["cat.sch.employees"]
    assert _failures_for(result, "cat.sch.employees") == ()


def test_resolve_propagates_block_through_a_diamond():
    # Given d depends on b and c; both b and c depend on a; a is missing a parent
    tables = (
        _table_with_fks("cat.sch.d", "cat.sch.b", "cat.sch.c"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table_with_fk("cat.sch.c", "cat.sch.a"),
        _table_with_fk("cat.sch.a", "cat.sch.missing"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then the block flows through both branches and d records both blocking FKs
    _assert_has_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        references="cat.sch.missing",
    )
    _assert_has_failure(
        result,
        "cat.sch.b",
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.a",
    )
    _assert_has_failure(
        result,
        "cat.sch.c",
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.a",
    )

    failures_for_d = _failures_for(result, "cat.sch.d")

    assert len(failures_for_d) == 2
    assert {
        (failure.local_columns, failure.references, failure.reason) for failure in failures_for_d
    } == {
        (
            ("b_id",),
            _qualified_name("cat.sch.b"),
            ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        ),
        (
            ("c_id",),
            _qualified_name("cat.sch.c"),
            ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        ),
    }


def test_resolve_records_no_fk_failure_for_table_passed_in_blocked_set():
    # Given one table is already blocked by an earlier phase
    table = _table("cat.sch.orders")
    blocked = {_qualified_name("cat.sch.orders")}

    # When resolving dependencies
    result = resolve((table,), blocked=blocked)

    # Then resolve does not claim ownership of that table's external failure
    assert _failures_for(result, "cat.sch.orders") == ()


def test_resolve_blocks_fk_dependent_of_a_blocked_table():
    # Given orders is blocked externally and shipments depends on orders
    orders = _table("cat.sch.orders")
    shipments = _table_with_fk("cat.sch.shipments", "cat.sch.orders")

    blocked = {_qualified_name("cat.sch.orders")}

    # When resolving dependencies
    result = resolve((orders, shipments), blocked=blocked)

    # Then shipments is blocked by orders, but orders gets no FK failure
    assert _failures_for(result, "cat.sch.orders") == ()
    _assert_has_failure(
        result,
        "cat.sch.shipments",
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.orders",
    )


def test_resolve_propagates_external_block_along_dependency_chain():
    # Given a is blocked externally, b depends on a, and c depends on b
    table_a = _table("cat.sch.a")
    table_b = _table_with_fk("cat.sch.b", "cat.sch.a")
    table_c = _table_with_fk("cat.sch.c", "cat.sch.b")

    blocked = {_qualified_name("cat.sch.a")}

    # When resolving dependencies
    result = resolve((table_a, table_b, table_c), blocked=blocked)

    # Then the externally blocked table gets no FK failure,
    # and the block propagates transitively
    assert _failures_for(result, "cat.sch.a") == ()
    _assert_has_failure(
        result,
        "cat.sch.b",
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.a",
    )
    _assert_has_failure(
        result,
        "cat.sch.c",
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.b",
    )


def test_resolve_fails_when_referenced_table_has_no_primary_key():
    # Given orders references customers, but customers has no primary key
    customers = _table(
        "cat.sch.customers",
        primary_key_columns=(),
        extra_columns=("id",),
    )
    orders = _table_with_fk("cat.sch.orders", "cat.sch.customers")

    # When resolving dependencies
    result = resolve((orders, customers))

    # Then orders fails because its FK does not target the parent's primary key
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.customers",
        local_columns=("ref_id",),
    )
    assert _failures_for(result, "cat.sch.customers") == ()


def test_resolve_fails_fk_whose_referenced_columns_are_not_the_pk():
    # Given customers' PK is id, but orders points at customers.email
    customers = DeltaTable(
        "cat",
        "sch",
        "customers",
        columns=(
            Column("id", String(), nullable=False),
            Column("email", String()),
        ),
        primary_key=["id"],
    ).to_desired_table()

    orders = DesiredTable(
        qualified_name=_qualified_name("cat.sch.orders"),
        columns=(
            Column("id", String(), nullable=False),
            Column("ref_email", String()),
        ),
        primary_key=PrimaryKeyConstraint.generate(
            table_name="orders",
            columns=("id",),
        ),
        foreign_keys=(
            ForeignKeyConstraint.generate(
                owner_table_name="orders",
                local_columns=("ref_email",),
                referenced_table=_qualified_name("cat.sch.customers"),
                referenced_columns=("email",),
            ),
        ),
    )

    # When resolving dependencies
    result = resolve((orders, customers))

    # Then orders is rejected because email is not customers' primary key
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.customers",
        local_columns=("ref_email",),
    )


def test_resolve_blocks_dependents_of_table_with_invalid_fk_target():
    # Given orders has an invalid FK target, and shipments depends on orders
    customers = _table(
        "cat.sch.customers",
        primary_key_columns=(),
        extra_columns=("id",),
    )
    orders = _table_with_fk("cat.sch.orders", "cat.sch.customers")
    shipments = _table_with_fk("cat.sch.shipments", "cat.sch.orders")

    # When resolving dependencies
    result = resolve((shipments, orders, customers))

    # Then orders fails directly and shipments is blocked by orders
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.customers",
    )
    _assert_has_failure(
        result,
        "cat.sch.shipments",
        reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        references="cat.sch.orders",
    )


def test_resolve_valid_chain_where_middle_table_has_pk_and_fk_executes():
    # Given c -> a -> b, and a has both its own PK and an FK to b
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.a"),
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table("cat.sch.b"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then the whole chain is valid and dependency-first ordered
    _assert_before(result, "cat.sch.b", "cat.sch.a")
    _assert_before(result, "cat.sch.a", "cat.sch.c")
    assert not result.fk_failures


def test_resolve_passes_when_fk_targets_composite_primary_key():
    # Given customers has a composite primary key
    customers = _table(
        "cat.sch.customers",
        primary_key_columns=("customer_id", "country_code"),
    )
    orders = _table_with_fk(
        "cat.sch.orders",
        "cat.sch.customers",
        local_columns=("customer_id", "country_code"),
        referenced_primary_key_columns=("customer_id", "country_code"),
    )

    # When resolving dependencies
    result = resolve((orders, customers))

    # Then the composite-key FK is valid
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not result.fk_failures


def test_resolve_fails_when_fk_references_only_part_of_composite_primary_key():
    # Given customers has a composite PK, but orders references only one PK column
    customers = _table(
        "cat.sch.customers",
        primary_key_columns=("customer_id", "country_code"),
    )

    orders = DesiredTable(
        qualified_name=_qualified_name("cat.sch.orders"),
        columns=(
            Column("id", String(), nullable=False),
            Column("customer_id", String()),
        ),
        primary_key=PrimaryKeyConstraint.generate(
            table_name="orders",
            columns=("id",),
        ),
        foreign_keys=(
            ForeignKeyConstraint.generate(
                owner_table_name="orders",
                local_columns=("customer_id",),
                referenced_table=_qualified_name("cat.sch.customers"),
                referenced_columns=("customer_id",),
            ),
        ),
    )

    # When resolving dependencies
    result = resolve((orders, customers))

    # Then the FK is rejected because it does not target the full parent PK
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.customers",
        local_columns=("customer_id",),
    )


def test_resolve_treats_composite_pk_referenced_column_order_as_irrelevant():
    # Given customers has a composite PK and the FK references both PK columns
    customers = _table(
        "cat.sch.customers",
        primary_key_columns=("customer_id", "country_code"),
    )

    orders = DesiredTable(
        qualified_name=_qualified_name("cat.sch.orders"),
        columns=(
            Column("id", String(), nullable=False),
            Column("customer_id", String()),
            Column("country_code", String()),
        ),
        primary_key=PrimaryKeyConstraint.generate(
            table_name="orders",
            columns=("id",),
        ),
        foreign_keys=(
            ForeignKeyConstraint.generate(
                owner_table_name="orders",
                local_columns=("country_code", "customer_id"),
                referenced_table=_qualified_name("cat.sch.customers"),
                referenced_columns=("country_code", "customer_id"),
            ),
        ),
    )

    # When resolving dependencies
    result = resolve((orders, customers))

    # Then the FK is valid because the referenced column set is the parent's PK
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not result.fk_failures
