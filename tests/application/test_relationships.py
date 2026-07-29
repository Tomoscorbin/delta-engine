"""
Unit tests for relationships.resolve().

These tests exercise the public resolver API rather than the graph traversal
implementation details. They cover dependency-first ordering, structural FK
failure classification, cycle detection, self-reference handling, and the
planned foreign-key actions carried on each resolution. Blocking dependents
of failed tables is the engine's gating walk, tested with the engine.
"""

import pytest

from delta_engine.application.failures import ForeignKeyFailureReason, ReadFailure
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.application.relationships import (
    ResolutionFailed,
    ResolutionSucceeded,
    ResolveResult,
    resolve,
)
from delta_engine.domain.model import ObservedColumn, ObservedTable, QualifiedName
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan import DropForeignKey, SetForeignKey
from delta_engine.schema import Column, DeltaTable, ForeignKey, Long, Self, String


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
    return [str(resolution.qualified_name) for resolution in result]


def _failures_for(result: ResolveResult, fqn: str) -> tuple:
    qualified_name = _qualified_name(fqn)
    for resolution in result:
        if resolution.qualified_name != qualified_name:
            continue
        if isinstance(resolution, ResolutionFailed):
            return resolution.failures
        return ()
    raise AssertionError(f"No resolution for {qualified_name}")


def _failed_resolutions(result: ResolveResult) -> tuple[ResolutionFailed, ...]:
    return tuple(resolution for resolution in result if isinstance(resolution, ResolutionFailed))


def _successful_resolution(result: ResolveResult, fqn: str) -> ResolutionSucceeded:
    qualified_name = _qualified_name(fqn)
    for resolution in result:
        if resolution.qualified_name != qualified_name:
            continue
        if isinstance(resolution, ResolutionSucceeded):
            return resolution
        raise AssertionError(f"Expected successful resolution for {qualified_name}")
    raise AssertionError(f"No resolution for {qualified_name}")


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


def _resolve_declared(tables: tuple[DesiredTable, ...]) -> ResolveResult:
    """Resolve declarations as if every table were absent from the catalog."""
    return resolve(tuple((table, TableAbsent()) for table in tables))


def test_resolve_with_empty_tables_returns_empty_result():
    # Given no registered tables
    # When resolving dependencies
    result = _resolve_declared(())

    # Then the result is empty
    assert result == ()


def test_resolution_failed_requires_at_least_one_failure():
    with pytest.raises(
        ValueError,
        match="ResolutionFailed requires at least one foreign-key failure",
    ):
        ResolutionFailed(
            qualified_name=_qualified_name("cat.sch.orders"),
            failures=(),
        )


def test_resolve_with_no_fks_preserves_prepared_input_order():
    # Given three independent tables in prepared order
    tables = (
        _table("cat.sch.a"),
        _table("cat.sch.b"),
        _table("cat.sch.c"),
    )

    # When resolving dependencies
    result = _resolve_declared(tables)

    # Then the independent tables stay in prepared input order
    assert _names(result) == ["cat.sch.a", "cat.sch.b", "cat.sch.c"]
    assert all(isinstance(resolution, ResolutionSucceeded) for resolution in result)


def test_resolve_ignores_foreign_keys_on_tag_scoped_declarations():
    # Given a tag-scoped declaration that carries an FK but does not manage FKs
    tables = (_tag_scoped_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When resolving dependencies
    result = _resolve_declared(tables)

    # Then the carried FK does not produce an unresolvable-reference failure
    assert _names(result) == ["cat.sch.orders"]
    assert not _failed_resolutions(result)


def test_resolve_does_not_order_by_unmanaged_foreign_keys():
    # Given a tag-scoped declaration listed before the table it references
    tables = (
        _tag_scoped_table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When resolving dependencies
    result = _resolve_declared(tables)

    # Then the unmanaged FK does not impose parent-before-child ordering
    assert _names(result) == ["cat.sch.orders", "cat.sch.customers"]
    assert not _failed_resolutions(result)


def test_resolve_orders_referenced_table_before_dependent():
    # Given orders depends on customers
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.customers"),
    )

    # When resolving dependencies
    result = _resolve_declared(tables)

    # Then customers appears before orders
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not _failed_resolutions(result)

    # And orders retains the resolved edge needed during execution
    [dependency] = _successful_resolution(result, "cat.sch.orders").dependencies
    assert dependency.referenced_table == _qualified_name("cat.sch.customers")
    assert dependency.blocked_failure.table == _qualified_name("cat.sch.orders")
    assert dependency.blocked_failure.local_columns == ("ref_id",)
    assert dependency.blocked_failure.references == _qualified_name("cat.sch.customers")
    assert dependency.blocked_failure.reason is ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY


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
    result = _resolve_declared(tables)

    # Then the referenced tag-scoped table is ordered before its managed dependent
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not _failed_resolutions(result)


def test_resolve_handles_chain_of_dependencies():
    # Given c -> b -> a
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.b"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table("cat.sch.a"),
    )

    # When resolving dependencies
    result = _resolve_declared(tables)

    # Then dependencies are ordered before their dependents
    _assert_before(result, "cat.sch.a", "cat.sch.b")
    _assert_before(result, "cat.sch.b", "cat.sch.c")
    assert not _failed_resolutions(result)


def test_resolve_orders_table_after_all_fk_parents():
    # Given order_items depends on both orders and products
    tables = (
        _table_with_fks("cat.sch.order_items", "cat.sch.orders", "cat.sch.products"),
        _table("cat.sch.products"),
        _table("cat.sch.orders"),
    )

    # When resolving dependencies
    result = _resolve_declared(tables)

    # Then both parent tables appear before the dependent table
    _assert_before(result, "cat.sch.orders", "cat.sch.order_items")
    _assert_before(result, "cat.sch.products", "cat.sch.order_items")
    assert not _failed_resolutions(result)


def test_resolve_fails_table_with_unresolvable_reference():
    # Given orders references customers but customers is not registered
    tables = (_table_with_fk("cat.sch.orders", "cat.sch.customers"),)

    # When resolving dependencies
    result = _resolve_declared(tables)

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
    result = _resolve_declared((shipments,))

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
    result = _resolve_declared(tables)

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
    result = _resolve_declared(tables)

    # Then every cycle member fails with a cycle failure
    for table_name in ("cat.sch.a", "cat.sch.b", "cat.sch.c"):
        _assert_has_failure(
            result,
            table_name,
            reason=ForeignKeyFailureReason.CYCLE,
        )

    assert set(_names(result)) == {"cat.sch.a", "cat.sch.b", "cat.sch.c"}


def test_resolve_records_cycle_failure_only_for_fk_inside_the_cycle():
    # Given a <-> b form a cycle, and a also references healthy table c
    tables = (
        _table_with_fks("cat.sch.a", "cat.sch.b", "cat.sch.c"),
        _table_with_fk("cat.sch.b", "cat.sch.a"),
        _table("cat.sch.c"),
    )

    # When resolving dependencies
    result = _resolve_declared(tables)

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
    # Given a <-> b form a cycle, but a's FK into b targets a non-primary-key
    # column. The FK-target check runs before cycle classification (see
    # _classify_structural_failures), so the structural problem is reported
    # per-FK even though a is also in a cycle.
    b = DesiredTable(
        qualified_name=_qualified_name("cat.sch.b"),
        columns=(
            Column("id", String(), nullable=False),
            Column("email", String()),
            Column("ref_id", String()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="b_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("ref_id",),
                referenced_table=_qualified_name("cat.sch.a"),
                referenced_columns=("id",),
                constraint_name="b_ref_id_fk",
            ),
        ),
    )
    a = DesiredTable(
        qualified_name=_qualified_name("cat.sch.a"),
        columns=(
            Column("id", String(), nullable=False),
            Column("ref_email", String()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="a_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("ref_email",),
                referenced_table=_qualified_name("cat.sch.b"),
                referenced_columns=("email",),
                constraint_name="a_ref_email_fk",
            ),
        ),
    )

    # When resolving dependencies
    result = _resolve_declared((a, b))

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


def test_resolve_does_not_fail_an_unrelated_sibling():
    # Given orders references a missing table and unrelated has no FKs
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.missing"),
        _table("cat.sch.unrelated"),
    )

    # When resolving dependencies
    result = _resolve_declared(tables)

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
    result = _resolve_declared((employees,))

    # Then the self-reference does not prevent the table from executing
    assert _names(result) == ["cat.sch.employees"]
    assert _failures_for(result, "cat.sch.employees") == ()
    assert _successful_resolution(result, "cat.sch.employees").dependencies == ()


def test_resolve_fails_when_referenced_table_has_no_primary_key():
    # Given orders references customers, but customers has no primary key
    customers = _table(
        "cat.sch.customers",
        primary_key_columns=(),
        extra_columns=("id",),
    )
    orders = _table_with_fk("cat.sch.orders", "cat.sch.customers")

    # When resolving dependencies
    result = _resolve_declared((orders, customers))

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
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("ref_email",),
                referenced_table=_qualified_name("cat.sch.customers"),
                referenced_columns=("email",),
                constraint_name="orders_ref_email_fk",
            ),
        ),
    )

    # When resolving dependencies
    result = _resolve_declared((orders, customers))

    # Then orders is rejected because email is not customers' primary key
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.customers",
        local_columns=("ref_email",),
    )


def _long_id_table(fqn: str) -> DesiredTable:
    """Build a table whose primary key column ``id`` is a Long rather than a String."""
    catalog, schema, table_name = _split_fqn(fqn)

    return DeltaTable(
        catalog,
        schema,
        table_name,
        columns=(Column("id", Long(), nullable=False),),
        primary_key=["id"],
    ).to_desired_table()


def test_resolve_fails_fk_whose_types_mismatch_the_registered_parent():
    # Given orders' FK was declared against a customers object whose id is a
    # String, but the customers declaration registered for this sync types id
    # as a Long
    orders = _table_with_fk("cat.sch.orders", "cat.sch.customers")
    customers = _long_id_table("cat.sch.customers")

    # When resolving dependencies
    result = _resolve_declared((orders, customers))

    # Then orders fails because ref_id's type does not match the registered
    # customers' primary key type, and customers itself is unaffected
    _assert_has_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH,
        references="cat.sch.customers",
        local_columns=("ref_id",),
    )
    assert _failures_for(result, "cat.sch.customers") == ()


def test_resolve_reports_type_mismatch_over_cycle_for_the_same_fk():
    # Given a <-> b form a cycle, and a's FK into b was declared against a
    # String-id b while the registered b types id as a Long. The type check
    # runs before cycle classification, so the structural problem is reported
    # per-FK even though a is also in a cycle.
    a = _table_with_fk("cat.sch.a", "cat.sch.b")
    b = DeltaTable(
        "cat",
        "sch",
        "b",
        columns=(
            Column("id", Long(), nullable=False),
            Column("ref_id", String()),
        ),
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(
                columns={"ref_id": "id"},
                references=_referenced_table("cat.sch.a"),
            )
        ],
    ).to_desired_table()

    # When resolving dependencies
    result = _resolve_declared((a, b))

    # Then a's single failure is the type mismatch, not a cycle failure,
    # while b's FK back into the cycle still fails as a cycle
    _assert_has_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH,
        references="cat.sch.b",
        local_columns=("ref_id",),
    )
    assert _failure_reasons_for(result, "cat.sch.a") == [
        ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH
    ]
    _assert_has_failure(
        result,
        "cat.sch.b",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.a",
    )


def test_resolve_valid_chain_where_middle_table_has_pk_and_fk_executes():
    # Given c -> a -> b, and a has both its own PK and an FK to b
    tables = (
        _table_with_fk("cat.sch.c", "cat.sch.a"),
        _table_with_fk("cat.sch.a", "cat.sch.b"),
        _table("cat.sch.b"),
    )

    # When resolving dependencies
    result = _resolve_declared(tables)

    # Then the whole chain is valid and dependency-first ordered
    _assert_before(result, "cat.sch.b", "cat.sch.a")
    _assert_before(result, "cat.sch.a", "cat.sch.c")
    assert not _failed_resolutions(result)


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
    result = _resolve_declared((orders, customers))

    # Then the composite-key FK is valid
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not _failed_resolutions(result)


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
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("customer_id",),
                referenced_table=_qualified_name("cat.sch.customers"),
                referenced_columns=("customer_id",),
                constraint_name="orders_customer_id_fk",
            ),
        ),
    )

    # When resolving dependencies
    result = _resolve_declared((orders, customers))

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
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("country_code", "customer_id"),
                referenced_table=_qualified_name("cat.sch.customers"),
                referenced_columns=("country_code", "customer_id"),
                constraint_name="orders_country_code_customer_id_fk",
            ),
        ),
    )

    # When resolving dependencies
    result = _resolve_declared((orders, customers))

    # Then the FK is valid because the referenced column set is the parent's PK
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not _failed_resolutions(result)


# ---------- resolve(): planned FK actions over read snapshots ----------


def _absent(desired: DesiredTable) -> tuple[DesiredTable, TableAbsent]:
    return (desired, TableAbsent())


def _present(desired: DesiredTable, observed: ObservedTable) -> tuple[DesiredTable, TablePresent]:
    return (desired, TablePresent(table=observed))


def _unreadable(desired: DesiredTable) -> tuple[DesiredTable, ReadFailure]:
    return (desired, ReadFailure(exception_type="ReadError", message="boom"))


def _observed(
    fqn: str,
    column_names: tuple[str, ...],
    foreign_keys: tuple[ForeignKeyConstraint, ...] = (),
) -> ObservedTable:
    return ObservedTable(
        qualified_name=_qualified_name(fqn),
        columns=tuple(ObservedColumn(name, String()) for name in column_names),
        foreign_keys=foreign_keys,
    )


def test_absent_table_plans_every_declared_foreign_key():
    # Given a child declaring one FK, with both tables absent from the catalog
    parent = _referenced_table("dev.silver.customers").to_desired_table()
    child = _table_with_fk("dev.silver.orders", "dev.silver.customers")

    # When resolved
    resolutions = resolve((_absent(parent), _absent(child)))

    # Then the child's resolution plans the declared FK
    child_resolution = _successful_resolution(resolutions, "dev.silver.orders")
    assert [type(action) for action in child_resolution.actions] == [SetForeignKey]


def test_present_table_diffs_foreign_keys_by_signature():
    # Given a child declaring FKs to customers and products, while the catalog
    # holds the customers FK (under a legacy name) plus one undeclared FK
    customers = _referenced_table("dev.silver.customers").to_desired_table()
    products = _referenced_table("dev.silver.products").to_desired_table()
    child = _table_with_fks("dev.silver.orders", "dev.silver.customers", "dev.silver.products")
    child_observed = _observed(
        "dev.silver.orders",
        ("id", "customers_id", "products_id", "legacy_ref"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("customers_id",),
                referenced_table=_qualified_name("dev.silver.customers"),
                referenced_columns=("id",),
                constraint_name="legacy_customers_fk",
            ),
            ForeignKeyConstraint(
                local_columns=("legacy_ref",),
                referenced_table=_qualified_name("dev.silver.archive"),
                referenced_columns=("id",),
                constraint_name="legacy_archive_fk",
            ),
        ),
    )

    # When resolved
    resolutions = resolve((_absent(customers), _absent(products), _present(child, child_observed)))

    # Then the declared-and-present FK yields no action, the declared-but-absent
    # FK yields SetForeignKey, and the undeclared observed FK yields DropForeignKey
    child_resolution = _successful_resolution(resolutions, "dev.silver.orders")
    set_actions = [a for a in child_resolution.actions if isinstance(a, SetForeignKey)]
    drop_actions = [a for a in child_resolution.actions if isinstance(a, DropForeignKey)]
    assert [str(a.constraint.referenced_table) for a in set_actions] == ["dev.silver.products"]
    assert [a.constraint.constraint_name for a in drop_actions] == ["legacy_archive_fk"]


def test_set_foreign_key_carries_catalog_spelling_on_both_sides():
    # Given child declares local column "customerId" that the catalog spells
    # "customerid", referencing parent column "Id" that the catalog spells "id"
    parent = _referenced_table(
        "dev.silver.customers", primary_key_columns=("Id",)
    ).to_desired_table()
    child = _table_with_fk(
        "dev.silver.orders",
        "dev.silver.customers",
        local_columns=("customerId",),
        referenced_primary_key_columns=("Id",),
    )
    parent_observed = _observed("dev.silver.customers", ("id",))
    child_observed = _observed("dev.silver.orders", ("id", "customerid"))

    # When resolved
    resolutions = resolve((_present(parent, parent_observed), _present(child, child_observed)))

    # Then the emitted SetForeignKey wears the catalogs' spellings on both sides
    child_resolution = _successful_resolution(resolutions, "dev.silver.orders")
    (action,) = child_resolution.actions
    assert isinstance(action, SetForeignKey)
    assert tuple(str(column) for column in action.constraint.local_columns) == ("customerid",)
    assert tuple(str(column) for column in action.constraint.referenced_columns) == ("id",)


def test_referenced_spelling_of_a_parent_created_this_sync_is_the_declared_one():
    # Given the referenced parent does not exist yet, so its declared spelling
    # is the only spelling there is
    parent = _referenced_table(
        "dev.silver.customers", primary_key_columns=("Id",)
    ).to_desired_table()
    child = _table_with_fk(
        "dev.silver.orders",
        "dev.silver.customers",
        referenced_primary_key_columns=("Id",),
    )

    # When resolved
    resolutions = resolve((_absent(parent), _absent(child)))

    # Then the referenced side keeps the declared spelling
    child_resolution = _successful_resolution(resolutions, "dev.silver.orders")
    (action,) = child_resolution.actions
    assert isinstance(action, SetForeignKey)
    assert tuple(str(column) for column in action.constraint.referenced_columns) == ("Id",)


def test_self_referencing_foreign_key_adopts_its_own_catalog_spelling():
    # Given a self-referencing key declared lowercase against camelCase catalog columns
    employees = DeltaTable(
        "dev",
        "silver",
        "employees",
        columns=(
            Column("id", String(), nullable=False),
            Column("managerid", String()),
        ),
        primary_key=["id"],
        foreign_keys=[ForeignKey(columns={"managerid": "id"}, references=Self)],
    ).to_desired_table()
    observed = _observed("dev.silver.employees", ("Id", "ManagerId"))

    # When resolved
    resolutions = resolve((_present(employees, observed),))

    # Then both sides wear the table's own catalog spelling
    employees_resolution = _successful_resolution(resolutions, "dev.silver.employees")
    (action,) = employees_resolution.actions
    assert isinstance(action, SetForeignKey)
    assert tuple(str(column) for column in action.constraint.local_columns) == ("ManagerId",)
    assert tuple(str(column) for column in action.constraint.referenced_columns) == ("Id",)


def test_foreign_key_to_a_renamed_parent_key_keeps_the_new_declared_name():
    # Given a parent renaming its key column — the catalog still spells the old
    # name — and a child referencing the declared new name
    parent = _referenced_table(
        "dev.silver.customers", primary_key_columns=("orderNumber",)
    ).to_desired_table()
    child = _table_with_fk(
        "dev.silver.orders",
        "dev.silver.customers",
        referenced_primary_key_columns=("orderNumber",),
    )
    parent_observed = _observed("dev.silver.customers", ("OrderId",))
    child_observed = _observed("dev.silver.orders", ("id", "ref_id"))

    # When resolved
    resolutions = resolve((_present(parent, parent_observed), _present(child, child_observed)))

    # Then the reference keeps the declared post-rename name, not the observed old one
    child_resolution = _successful_resolution(resolutions, "dev.silver.orders")
    (action,) = child_resolution.actions
    assert isinstance(action, SetForeignKey)
    assert tuple(str(column) for column in action.constraint.referenced_columns) == ("orderNumber",)


def test_unreadable_table_contributes_edges_but_no_actions():
    # Given a child declaring an FK whose own catalog state could not be read
    parent = _referenced_table("dev.silver.customers").to_desired_table()
    child = _table_with_fk("dev.silver.orders", "dev.silver.customers")

    # When resolved
    resolutions = resolve((_absent(parent), _unreadable(child)))

    # Then no actions are planned against unknown state, but the dependency
    # edge still exists so gating can block on the parent's outcome
    child_resolution = _successful_resolution(resolutions, "dev.silver.orders")
    assert child_resolution.actions == ()
    assert [str(d.referenced_table) for d in child_resolution.dependencies] == [
        "dev.silver.customers"
    ]
    blocking = child_resolution.blocking_failures({_qualified_name("dev.silver.customers")})
    assert [failure.reason for failure in blocking] == [
        ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY
    ]
    assert child_resolution.blocking_failures(frozenset()) == ()


def test_structural_failure_still_carries_planned_actions_for_the_preview():
    # Given a child whose FK references an unregistered table
    child = _table_with_fk("dev.silver.orders", "dev.silver.archive")

    # When resolved
    resolutions = resolve((_absent(child),))

    # Then the failure is structural and the planned action is still stated,
    # declared-spelled, for the report preview
    (resolution,) = resolutions
    assert isinstance(resolution, ResolutionFailed)
    assert [failure.reason for failure in resolution.failures] == [
        ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    ]
    assert [type(action) for action in resolution.actions] == [SetForeignKey]


def test_resolutions_come_dependency_first():
    # Given a child declared before its parent
    parent = _referenced_table("dev.silver.customers").to_desired_table()
    child = _table_with_fk("dev.silver.orders", "dev.silver.customers")

    # When resolved with the child first
    resolutions = resolve((_absent(child), _absent(parent)))

    # Then the parent still precedes the child
    _assert_before(resolutions, "dev.silver.customers", "dev.silver.orders")


def test_unmanaged_foreign_keys_create_no_edges_but_actions_are_still_stated():
    # Given a tag-scoped table declaring an FK it does not manage
    table = _tag_scoped_table_with_fk("dev.silver.orders", "dev.silver.customers")

    # When resolved
    resolutions = resolve((_absent(table),))

    # Then no dependency edge exists and nothing failed structurally, but the
    # action is still stated for validation's scope gate to judge
    (resolution,) = resolutions
    assert isinstance(resolution, ResolutionSucceeded)
    assert resolution.dependencies == ()
    assert [type(action) for action in resolution.actions] == [SetForeignKey]


def test_resolve_classifies_the_four_structural_reasons():
    # Unresolvable reference: the parent is not registered in this sync
    orphan = _table_with_fk("dev.silver.orders", "dev.silver.archive")
    result = resolve((_absent(orphan),))
    _assert_has_failure(
        result, "dev.silver.orders", reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE
    )

    # Not a key: the FK targets columns that are not the registered parent's PK
    child = _table_with_fk("dev.silver.orders", "dev.silver.customers")
    odd_parent = _referenced_table(
        "dev.silver.customers", primary_key_columns=("customer_key",)
    ).to_desired_table()
    result = resolve((_absent(child), _absent(odd_parent)))
    _assert_has_failure(
        result, "dev.silver.orders", reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY
    )

    # Type mismatch: the registered parent's key column has a different type
    child = _table_with_fk("dev.silver.orders", "dev.silver.customers")
    long_parent = DeltaTable(
        "dev",
        "silver",
        "customers",
        columns=(Column("id", Long(), nullable=False),),
        primary_key=["id"],
    ).to_desired_table()
    result = resolve((_absent(child), _absent(long_parent)))
    _assert_has_failure(
        result,
        "dev.silver.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH,
    )

    # Cycle: two tables reference each other
    invoices = _table_with_fk("dev.silver.invoices", "dev.silver.ledger")
    ledger = _table_with_fk("dev.silver.ledger", "dev.silver.invoices")
    result = resolve((_absent(invoices), _absent(ledger)))
    _assert_has_failure(result, "dev.silver.invoices", reason=ForeignKeyFailureReason.CYCLE)
    _assert_has_failure(result, "dev.silver.ledger", reason=ForeignKeyFailureReason.CYCLE)
