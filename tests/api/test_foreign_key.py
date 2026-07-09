import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.schema import Column, DeltaTable, ForeignKey, Integer, Long, Self, String


def _customers() -> DeltaTable:
    return DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False, primary_key=True)],
    )


def test_foreign_key_declaration_has_only_local_columns_and_references():
    # Given / When / Then the public declaration rejects internal fields
    with pytest.raises(TypeError):
        ForeignKey(  # type: ignore[call-arg]
            local_columns=("customer_id",),
            references=_customers(),
            referenced_columns=("id",),
        )
    with pytest.raises(TypeError):
        ForeignKey(  # type: ignore[call-arg]
            local_columns=("customer_id",),
            references=_customers(),
            constraint_name="orders_customer_id_fk",
        )


def test_delta_table_infers_referenced_columns_from_referenced_primary_key():
    # Given a referenced table with a single-column primary key
    customers = _customers()
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[ForeignKey(local_columns=("customer_id",), references=customers)],
    )

    # Then the FK is lowered with the referenced primary key inferred
    [foreign_key] = orders.foreign_keys
    assert foreign_key.referenced_table == QualifiedName("cat", "sch", "customers")
    assert foreign_key.referenced_columns == ("id",)
    assert foreign_key.constraint_name == "orders_customer_id_fk"


def test_delta_table_infers_composite_primary_key_in_declaration_order():
    # Given a referenced table with a composite primary key (tenant_id, id)
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[
            Column("tenant_id", Integer(), nullable=False, primary_key=True),
            Column("id", Integer(), nullable=False, primary_key=True),
        ],
    )
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[
            Column("tenant_id", Integer()),
            Column("customer_id", Integer()),
        ],
        foreign_keys=[ForeignKey(local_columns=("tenant_id", "customer_id"), references=customers)],
    )

    # Then referenced columns follow the referenced PK's declaration order
    [foreign_key] = orders.foreign_keys
    assert foreign_key.local_columns == ("tenant_id", "customer_id")
    assert foreign_key.referenced_columns == ("tenant_id", "id")


def test_delta_table_supports_self_referential_foreign_key():
    # Given a table referencing its own primary key via the Self sentinel
    employee = DeltaTable(
        catalog="cat",
        schema="sch",
        name="employee",
        columns=[
            Column("id", Integer(), nullable=False, primary_key=True),
            Column("manager_id", Integer()),
        ],
        foreign_keys=[ForeignKey(local_columns=("manager_id",), references=Self)],
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
            foreign_keys=[ForeignKey(local_columns=("customer_id",), references=customers)],
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
            foreign_keys=[ForeignKey(local_columns=("manager_id",), references=Self)],
        )


def test_delta_table_rejects_local_column_count_mismatch():
    # Given a referenced single-column PK but two local columns
    customers = _customers()

    # When / Then the arity mismatch is rejected with both counts named
    with pytest.raises(ValueError, match="primary key"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[Column("a", Integer()), Column("b", Integer())],
            foreign_keys=[ForeignKey(local_columns=("a", "b"), references=customers)],
        )


def test_delta_table_rejects_cross_catalog_foreign_key():
    # Given a referenced table that lives in a different catalog
    customers = DeltaTable(
        catalog="other",
        schema="sch",
        name="customers",
        columns=[Column("id", Integer(), nullable=False, primary_key=True)],
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
            foreign_keys=[ForeignKey(local_columns=("customer_id",), references=customers)],
        )


def test_delta_table_rejects_foreign_keys_whose_generated_names_collide():
    # Given two FKs over different local columns whose generated constraint
    # names collide: ('a', 'b_c') and ('a_b', 'c') both derive orders_a_b_c_fk
    parts = DeltaTable(
        catalog="cat",
        schema="sch",
        name="parts",
        columns=[
            Column("x", Integer(), nullable=False, primary_key=True),
            Column("y", Integer(), nullable=False, primary_key=True),
        ],
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
                ForeignKey(local_columns=("a", "b_c"), references=parts),
                ForeignKey(local_columns=("a_b", "c"), references=parts),
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
                ForeignKey(local_columns=("customer_id",), references="cat.sch.customers")  # type: ignore[arg-type]
            ],
        )


def test_foreign_key_rejects_local_column_type_mismatch_with_target_primary_key():
    # Given a referenced primary key of type Long and a local column of type String
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[Column("id", Long(), nullable=False, primary_key=True)],
    )

    # When / Then construction fails because the local column type does not match
    with pytest.raises(ValueError, match="type mismatch"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("id", Long(), nullable=False, primary_key=True),
                Column("customer_id", String()),
            ],
            foreign_keys=[ForeignKey(local_columns=("customer_id",), references=customers)],
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
                Column("id", Long(), nullable=False, primary_key=True),
                Column("manager_id", Integer()),
            ],
            foreign_keys=[ForeignKey(local_columns=("manager_id",), references=Self)],
        )


def test_composite_foreign_key_rejects_a_single_mismatched_column_pair():
    # Given a composite referenced primary key where only the second local
    # column's type differs
    customers = DeltaTable(
        catalog="cat",
        schema="sch",
        name="customers",
        columns=[
            Column("tenant_id", Integer(), nullable=False, primary_key=True),
            Column("id", Long(), nullable=False, primary_key=True),
        ],
    )

    # When / Then construction fails naming the mismatched pair
    with pytest.raises(ValueError, match=r"orders\.customer_id"):
        DeltaTable(
            catalog="cat",
            schema="sch",
            name="orders",
            columns=[
                Column("id", Long(), nullable=False, primary_key=True),
                Column("customer_tenant_id", Integer()),
                Column("customer_id", String()),
            ],
            foreign_keys=[
                ForeignKey(
                    local_columns=("customer_tenant_id", "customer_id"),
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
        columns=[Column("id", Long(), nullable=False, primary_key=True)],
    )
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[
            Column("id", Long(), nullable=False, primary_key=True),
            Column("customer_id", Long()),
        ],
        foreign_keys=[ForeignKey(local_columns=("customer_id",), references=customers)],
    )

    # Then the foreign key lowers normally
    assert orders.foreign_keys[0].local_columns == ("customer_id",)


def test_foreign_key_accepts_local_columns_as_a_list():
    # Given a foreign key declared with a plain list (the natural call style)
    customers = _customers()
    declaration = ForeignKey(local_columns=["customer_id"], references=customers)
    orders = DeltaTable(
        catalog="cat",
        schema="sch",
        name="orders",
        columns=[Column("id", Integer()), Column("customer_id", Integer())],
        foreign_keys=[declaration],
    )

    # Then the declaration and the lowered constraint both hold immutable tuples
    assert declaration.local_columns == ("customer_id",)
    [constraint] = orders.foreign_keys
    assert constraint.local_columns == ("customer_id",)
