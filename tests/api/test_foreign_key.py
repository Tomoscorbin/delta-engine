from types import MappingProxyType

import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.schema import Column, DeltaTable, ForeignKey, Integer, Long, Self, String


def _customers() -> DeltaTable:
    return DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )


def test_foreign_key_declaration_has_only_columns_and_references():
    # Given / When / Then the public declaration rejects internal fields
    with pytest.raises(TypeError):
        ForeignKey(  # type: ignore[call-arg]
            columns={"customer_id": "id"},
            references=_customers(),
            referenced_columns=("id",),
        )
    with pytest.raises(TypeError):
        ForeignKey(  # type: ignore[call-arg]
            columns={"customer_id": "id"},
            references=_customers(),
            constraint_name="orders_customer_id_fk",
        )


def test_delta_table_stores_composite_foreign_key_canonically():
    # Given a referenced table with a composite primary key (tenant_id, id)
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[
            Column("tenant_id", Integer()),
            Column("customer_id", Integer()),
        ],
        foreign_keys=[
            ForeignKey(
                columns={"tenant_id": "tenant_id", "customer_id": "id"},
                references=customers,
            )
        ],
    )

    # Then pairs are stored canonically (sorted by local column), pairing intact
    [foreign_key] = orders.foreign_keys
    assert foreign_key.local_columns == ("customer_id", "tenant_id")
    assert foreign_key.referenced_columns == ("id", "tenant_id")


def test_delta_table_supports_self_referential_foreign_key():
    # Given a table referencing its own primary key via the Self sentinel
    employee = DeltaTable(
        catalog="cat",
        schema="sch",
        name="employee",
        columns=[
            Column("id", Integer(), nullable=False),
            Column("manager_id", Integer()),
        ],
        primary_key=["id"],
        foreign_keys=[ForeignKey(columns={"manager_id": "id"}, references=Self)],
    )

    # Then the FK targets the table's own qualified name and primary key
    [foreign_key] = employee.foreign_keys
    assert foreign_key.referenced_table == QualifiedName("cat", "sch", "employee")
    assert foreign_key.referenced_columns == ("id",)


def test_delta_table_rejects_reference_to_table_with_no_primary_key():
    # Given a referenced table with no primary key
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer())],
    )

    # When / Then construction fails because there is no key to infer
    with pytest.raises(ValueError, match="no primary key"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("id", Integer()), Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def test_delta_table_rejects_self_reference_without_primary_key():
    # Given a table with no primary key that references itself
    # When / Then the same no-primary-key error names the table
    with pytest.raises(ValueError, match="no primary key"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="employee",
            columns=[Column("manager_id", Integer())],
            foreign_keys=[ForeignKey(columns={"manager_id": "id"}, references=Self)],
        )


def test_delta_table_rejects_cross_catalog_foreign_key():
    # Given a referenced table that lives in a different catalog
    customers = DeltaTable(
        catalog="other",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False)],
        primary_key=["id"],
    )

    # When / Then the declaration is rejected: information_schema is
    # per-catalog, so the engine could create the constraint but never
    # observe it, and every later sync would re-plan and fail.
    with pytest.raises(ValueError, match="cross-catalog"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def test_delta_table_rejects_foreign_keys_whose_generated_names_collide():
    # Given two FKs over different local columns whose generated constraint
    # names collide: ('a', 'b_c') and ('a_b', 'c') both derive orders_a_b_c_fk
    parts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="parts",
        columns=[
            Column("x", Integer(), nullable=False),
            Column("y", Integer(), nullable=False),
        ],
        primary_key=["x", "y"],
    )

    # When / Then the collision is rejected at declaration time
    with pytest.raises(ValueError, match="same constraint name"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("a", Integer()),
                Column("b_c", Integer()),
                Column("a_b", Integer()),
                Column("c", Integer()),
            ],
            foreign_keys=[
                ForeignKey(columns={"a": "x", "b_c": "y"}, references=parts),
                ForeignKey(columns={"a_b": "x", "c": "y"}, references=parts),
            ],
        )


def test_delta_table_rejects_non_table_reference():
    # Given a reference that is neither a DeltaTable nor Self
    # When / Then a TypeError names the accepted types
    with pytest.raises(TypeError, match="DeltaTable or Self"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("customer_id", Integer())],
            foreign_keys=[
                ForeignKey(columns={"customer_id": "id"}, references="cat.sch.customers")  # type: ignore[arg-type]
            ],
        )


def test_foreign_key_rejects_local_column_type_mismatch_with_target_primary_key():
    # Given a referenced primary key of type Long and a local column of type String
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Long(), nullable=False)],
        primary_key=["id"],
    )

    # When / Then construction fails because the local column type does not match
    with pytest.raises(ValueError, match="type mismatch"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("id", Long(), nullable=False),
                Column("customer_id", String()),
            ],
            primary_key=["id"],
            foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
        )


def test_self_referential_foreign_key_rejects_type_mismatch():
    # Given a self-referential foreign key whose local column type differs from the primary key
    # When / Then construction fails because the local column type does not match
    with pytest.raises(ValueError, match="type mismatch"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="employees",
            columns=[
                Column("id", Long(), nullable=False),
                Column("manager_id", Integer()),
            ],
            primary_key=["id"],
            foreign_keys=[ForeignKey(columns={"manager_id": "id"}, references=Self)],
        )


def test_composite_foreign_key_rejects_a_single_mismatched_column_pair():
    # Given a composite referenced primary key where only the second local
    # column's type differs
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Long(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )

    # When / Then construction fails naming the mismatched pair
    with pytest.raises(ValueError, match=r"orders\.customer_id"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("id", Long(), nullable=False),
                Column("customer_tenant_id", Integer()),
                Column("customer_id", String()),
            ],
            primary_key=["id"],
            foreign_keys=[
                ForeignKey(
                    columns={"customer_tenant_id": "tenant_id", "customer_id": "id"},
                    references=customers,
                )
            ],
        )


def test_foreign_key_with_matching_types_still_lowers():
    # Given a referenced primary key and local column that share the same type
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Long(), nullable=False)],
        primary_key=["id"],
    )
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[
            Column("id", Long(), nullable=False),
            Column("customer_id", Long()),
        ],
        primary_key=["id"],
        foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
    )

    # Then the foreign key lowers normally
    assert orders.foreign_keys[0].local_columns == ("customer_id",)


def test_foreign_key_accepts_columns_as_any_mapping():
    # Given columns supplied as a read-only mapping rather than a dict
    customers = _customers()
    declaration = ForeignKey(columns=MappingProxyType({"customer_id": "id"}), references=customers)
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[declaration],
    )

    # Then the declaration copies the mapping and the lowered constraint holds an immutable tuple
    assert dict(declaration.columns) == {"customer_id": "id"}
    [constraint] = orders.foreign_keys
    assert constraint.local_columns == ("customer_id",)


def test_mapping_lowers_single_column_foreign_key():
    customers = _customers()
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("customer_id", Integer())],
        foreign_keys=[ForeignKey(columns={"customer_id": "id"}, references=customers)],
    )

    [foreign_key] = orders.foreign_keys
    assert foreign_key.local_columns == ("customer_id",)
    assert foreign_key.referenced_table == QualifiedName("cat", "sch", "customers")
    assert foreign_key.referenced_columns == ("id",)
    assert foreign_key.constraint_name == "orders_customer_id_fk"


def test_mapping_lowers_composite_foreign_key_with_stated_pairing():
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("tenant_id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[
            ForeignKey(
                columns={"tenant_id": "tenant_id", "customer_id": "id"},
                references=accounts,
            )
        ],
    )

    # Stored canonically (sorted by local column), pairing exactly as stated
    [foreign_key] = orders.foreign_keys
    assert foreign_key.local_columns == ("customer_id", "tenant_id")
    assert foreign_key.referenced_columns == ("id", "tenant_id")


def test_mapping_insertion_order_is_irrelevant():
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
        ],
        primary_key=["tenant_id", "id"],
    )

    def orders_with(mapping):
        return DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("tenant_id", Integer()), Column("customer_id", Integer())],
            foreign_keys=[ForeignKey(columns=mapping, references=accounts)],
        ).foreign_keys[0]

    one = orders_with({"tenant_id": "tenant_id", "customer_id": "id"})
    two = orders_with({"customer_id": "id", "tenant_id": "tenant_id"})
    assert one == two
    assert one.constraint_name == two.constraint_name


def test_non_mapping_columns_is_rejected_with_pointed_message():
    # The old list shape must fail loudly at the fix, not obscurely later
    with pytest.raises(TypeError, match="local column: referenced column"):
        ForeignKey(columns=["customer_id"], references=_customers())  # type: ignore[arg-type]


def test_mapping_not_covering_the_key_is_rejected():
    # Given a mapping missing one key column and naming a non-key column
    accounts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="accounts",
        columns=[
            Column("tenant_id", Integer(), nullable=False),
            Column("id", Integer(), nullable=False),
            Column("region", Integer()),
        ],
        primary_key=["tenant_id", "id"],
    )

    with pytest.raises(ValueError) as excinfo:
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("tenant_id", Integer()), Column("region_ref", Integer())],
            foreign_keys=[
                ForeignKey(
                    columns={"tenant_id": "tenant_id", "region_ref": "region"},
                    references=accounts,
                )
            ],
        )

    # Both sides named: what's missing from the mapping, what isn't in the key
    assert "id" in str(excinfo.value)
    assert "region" in str(excinfo.value)


def test_two_locals_mapped_to_the_same_key_column_are_rejected():
    with pytest.raises(ValueError, match="Duplicate foreign key referenced column"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("a", Integer()), Column("b", Integer())],
            foreign_keys=[ForeignKey(columns={"a": "id", "b": "id"}, references=_customers())],
        )


def test_reordering_the_parent_primary_key_produces_no_foreign_key_drift():
    # Regression for the parent-reorder trap: the child's mapping is explicit,
    # so a parent primary_key list reorder must be a no-op end to end.
    from delta_engine.domain.plan.diff import TableDrift, diff_table

    def child_of(parent_key_order):
        accounts = DeltaTable(
            catalog="cat",
            schema="sch",
            name="accounts",
            columns=[
                Column("tenant_id", Integer(), nullable=False),
                Column("id", Integer(), nullable=False),
            ],
            primary_key=parent_key_order,
        )
        return DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("tenant_id", Integer()), Column("customer_id", Integer())],
            foreign_keys=[
                ForeignKey(
                    columns={"tenant_id": "tenant_id", "customer_id": "id"},
                    references=accounts,
                )
            ],
        ).to_desired_table()

    before = child_of(["tenant_id", "id"])
    after = child_of(["id", "tenant_id"])
    assert before.foreign_keys == after.foreign_keys

    from delta_engine.domain.model import ObservedTable
    from tests.builders import as_observed_columns

    observed = ObservedTable(
        qualified_name=before.qualified_name,
        columns=as_observed_columns(before.columns),
        foreign_keys=before.foreign_keys,
    )
    diff = diff_table(after, observed)
    assert isinstance(diff, TableDrift)
    assert diff.changes == ()
