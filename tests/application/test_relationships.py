"""
Unit tests for relationships.resolve().

These tests exercise the public resolver API rather than the graph traversal
implementation details. They cover dependency-first ordering, structural FK
failure classification, cycle detection, self-reference handling, the
dependency edges each resolution retains, and the blocking those edges name
for a given set of tables that will not converge. The resolver is pure
declaration analysis — the world is consulted later, so how blocking
propagates across a whole run is tested with the engine.
"""

from delta_engine.application.failures import ForeignKeyFailure, ForeignKeyFailureReason
from delta_engine.application.relationships import TableResolution, resolve
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint
from delta_engine.domain.model.table import DesiredTable
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


def _names(result: tuple[TableResolution, ...]) -> list[str]:
    return [str(resolution.qualified_name) for resolution in result]


def _resolution_for(result: tuple[TableResolution, ...], fqn: str) -> TableResolution:
    qualified_name = _qualified_name(fqn)
    for resolution in result:
        if resolution.qualified_name == qualified_name:
            return resolution
    raise AssertionError(f"No resolution for {qualified_name}")


def _failures_for(result: tuple[TableResolution, ...], fqn: str) -> tuple[ForeignKeyFailure, ...]:
    return _resolution_for(result, fqn).structural_failures


def _failed_resolutions(result: tuple[TableResolution, ...]) -> tuple[TableResolution, ...]:
    return tuple(resolution for resolution in result if resolution.structural_failures)


def _sound_resolution(result: tuple[TableResolution, ...], fqn: str) -> TableResolution:
    resolution = _resolution_for(result, fqn)
    assert resolution.structural_failures == ()
    return resolution


def _assert_before(result: tuple[TableResolution, ...], parent: str, child: str) -> None:
    names = _names(result)
    assert names.index(parent) < names.index(child)


def _assert_only_failure(
    result: tuple[TableResolution, ...],
    fqn: str,
    *,
    reason: ForeignKeyFailureReason,
    references: str,
    local_columns: tuple[str, ...] = ("ref_id",),
) -> None:
    assert _failures_for(result, fqn) == (
        ForeignKeyFailure(
            table=_qualified_name(fqn),
            local_columns=local_columns,
            references=_qualified_name(references),
            reason=reason,
        ),
    )


def test_resolve_with_empty_tables_returns_empty_result():
    # Given no registered tables
    # When resolving dependencies
    result = resolve(())

    # Then the result is empty
    assert result == ()


def test_each_resolution_carries_the_declaration_it_was_judged_from():
    # Given two tables whose dependency reverses their input order
    parent = _table("cat.sch.customers")
    child = _table_with_fk("cat.sch.orders", "cat.sch.customers")

    # When resolving dependencies
    result = resolve((child, parent))

    # Then each resolution carries its own declaration, so callers need no lookup
    assert [resolution.desired for resolution in result] == [parent, child]


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
    assert not _failed_resolutions(result)


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
    assert not _failed_resolutions(result)
    assert _sound_resolution(result, "cat.sch.orders").dependencies == ()


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
    assert not _failed_resolutions(result)

    # And orders retains the dependency edge — the declared constraint itself
    [dependency] = _sound_resolution(result, "cat.sch.orders").dependencies
    assert dependency.referenced_table == _qualified_name("cat.sch.customers")
    assert tuple(str(column) for column in dependency.local_columns) == ("ref_id",)


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
    assert not _failed_resolutions(result)


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
    assert not _failed_resolutions(result)


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
    assert not _failed_resolutions(result)


def test_resolve_keeps_unresolvable_reference_failure_local_to_its_table():
    # Given orders references an unregistered table alongside an unrelated table
    tables = (
        _table_with_fk("cat.sch.orders", "cat.sch.customers"),
        _table("cat.sch.unrelated"),
    )

    # When resolving dependencies
    result = resolve(tables)

    # Then the exact FK fails without affecting the unrelated table
    _assert_only_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.UNRESOLVABLE_REFERENCE,
        references="cat.sch.customers",
    )
    _sound_resolution(result, "cat.sch.unrelated")


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
    for table_name, reference in (
        ("cat.sch.a", "cat.sch.b"),
        ("cat.sch.b", "cat.sch.c"),
        ("cat.sch.c", "cat.sch.a"),
    ):
        _assert_only_failure(
            result,
            table_name,
            reason=ForeignKeyFailureReason.CYCLE,
            references=reference,
        )

    assert sorted(_names(result)) == ["cat.sch.a", "cat.sch.b", "cat.sch.c"]


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
    _assert_only_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.b",
        local_columns=("b_id",),
    )
    _assert_only_failure(
        result,
        "cat.sch.b",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.a",
    )
    _sound_resolution(result, "cat.sch.c")


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
    result = resolve((a, b))

    # Then a's single failure is the invalid FK target, not a cycle failure
    _assert_only_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.b",
        local_columns=("ref_email",),
    )


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
    assert _resolution_for(result, "cat.sch.employees").dependencies == ()


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
    _assert_only_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.customers",
    )
    _sound_resolution(result, "cat.sch.customers")


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
    result = resolve((orders, customers))

    # Then orders is rejected because email is not customers' primary key
    _assert_only_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMNS_NOT_A_KEY,
        references="cat.sch.customers",
        local_columns=("ref_email",),
    )


def test_resolve_fails_fk_whose_types_mismatch_the_registered_parent():
    # Given orders' FK was declared against a customers object whose id is a
    # String, but the customers declaration registered for this sync types id
    # as a Long
    orders = _table_with_fk("cat.sch.orders", "cat.sch.customers")
    customers = _long_id_table("cat.sch.customers")

    # When resolving dependencies
    result = resolve((orders, customers))

    # Then orders fails because ref_id's type does not match the registered
    # customers' primary key type, and customers itself is unaffected
    _assert_only_failure(
        result,
        "cat.sch.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH,
        references="cat.sch.customers",
    )
    _sound_resolution(result, "cat.sch.customers")


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
    result = resolve((a, b))

    # Then a's single failure is the type mismatch, not a cycle failure,
    # while b's FK back into the cycle still fails as a cycle
    _assert_only_failure(
        result,
        "cat.sch.a",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMN_TYPE_MISMATCH,
        references="cat.sch.b",
    )
    _assert_only_failure(
        result,
        "cat.sch.b",
        reason=ForeignKeyFailureReason.CYCLE,
        references="cat.sch.a",
    )


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
    result = resolve((orders, customers))

    # Then the FK is rejected because it does not target the full parent PK
    _assert_only_failure(
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
    result = resolve((orders, customers))

    # Then the FK is valid because the referenced column set is the parent's PK
    _assert_before(result, "cat.sch.customers", "cat.sch.orders")
    assert not _failed_resolutions(result)


def test_foreign_key_referenced_column_case_must_match_the_registered_declaration():
    # Given a registered parent declaring its key as "OrderId", and a child whose
    # FK was declared against a parent object spelling the same key "ORDERID"
    parent = _referenced_table(
        "dev.silver.customers", primary_key_columns=("OrderId",)
    ).to_desired_table()
    child = _table_with_fk(
        "dev.silver.orders",
        "dev.silver.customers",
        referenced_primary_key_columns=("ORDERID",),
    )

    # When resolved
    resolutions = resolve((parent, child))

    # Then the FK fails structurally: ADD CONSTRAINT resolves case-sensitively,
    # so the reference must wear the registered declaration's exact spelling
    _assert_only_failure(
        resolutions,
        "dev.silver.orders",
        reason=ForeignKeyFailureReason.REFERENCED_COLUMN_CASE_MISMATCH,
        references="dev.silver.customers",
    )


def test_foreign_key_referenced_spelling_matching_exactly_is_sound():
    # Given the same shape with the reference spelled exactly as registered
    parent = _referenced_table(
        "dev.silver.customers", primary_key_columns=("OrderId",)
    ).to_desired_table()
    child = _table_with_fk(
        "dev.silver.orders",
        "dev.silver.customers",
        referenced_primary_key_columns=("OrderId",),
    )

    resolutions = resolve((parent, child))

    _sound_resolution(resolutions, "dev.silver.orders")


def test_a_resolution_is_blocked_only_by_its_unconverged_dependencies():
    # Given orders depends on customers, alongside an unrelated table
    resolutions = resolve(
        (
            _table_with_fk("cat.sch.orders", "cat.sch.customers"),
            _table("cat.sch.customers"),
            _table("cat.sch.unrelated"),
        )
    )
    orders = _sound_resolution(resolutions, "cat.sch.orders")

    # When both tables will not converge
    blocking = orders.blocked_by(
        {_qualified_name("cat.sch.customers"), _qualified_name("cat.sch.unrelated")}
    )

    # Then only its dependency blocks it
    assert blocking == (
        ForeignKeyFailure(
            table=_qualified_name("cat.sch.orders"),
            local_columns=("ref_id",),
            references=_qualified_name("cat.sch.customers"),
            reason=ForeignKeyFailureReason.BLOCKED_BY_FAILED_DEPENDENCY,
        ),
    )
    assert orders.blocked_by(set()) == ()


def test_a_resolution_names_every_blocked_dependency_separately():
    # Given shipments depends on both orders and customers, and both fail
    resolutions = resolve(
        (
            _table_with_fks("cat.sch.shipments", "cat.sch.orders", "cat.sch.customers"),
            _table("cat.sch.orders"),
            _table("cat.sch.customers"),
        )
    )
    shipments = _sound_resolution(resolutions, "cat.sch.shipments")

    # When both dependencies will not converge
    blocking = shipments.blocked_by(
        {_qualified_name("cat.sch.orders"), _qualified_name("cat.sch.customers")}
    )

    # Then each blocked edge is reported on its own, naming its own columns
    assert {(failure.references, failure.local_columns) for failure in blocking} == {
        (_qualified_name("cat.sch.orders"), ("orders_id",)),
        (_qualified_name("cat.sch.customers"), ("customers_id",)),
    }
