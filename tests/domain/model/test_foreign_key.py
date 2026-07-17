import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.constraints import ForeignKeyConstraint


def _customers() -> QualifiedName:
    return QualifiedName("main", "sales", "customers")


def test_signature_ignores_constraint_name():
    # Given two FKs with identical content but different names
    one = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
        constraint_name="orders_customer_id_fk",
    )
    two = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
        constraint_name="chosen_elsewhere",
    )

    # Then their signatures are equal — name is not part of content identity
    assert one.signature == two.signature


def test_signature_differs_when_referenced_table_differs():
    # Given two FKs that differ only in the referenced table
    to_old = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("main", "sales", "old_customers"),
        referenced_columns=("id",),
        constraint_name="orders_customer_id_fk",
    )
    to_new = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("main", "sales", "new_customers"),
        referenced_columns=("id",),
        constraint_name="orders_customer_id_fk",
    )

    # Then their signatures differ
    assert to_old.signature != to_new.signature


def test_rejects_empty_local_columns():
    # Given / When / Then an empty local-column tuple is rejected
    with pytest.raises(ValueError, match="local_columns must not be empty"):
        ForeignKeyConstraint(
            local_columns=(),
            referenced_table=_customers(),
            referenced_columns=("id",),
            constraint_name="x_fk",
        )


def test_rejects_empty_referenced_columns():
    # Given / When / Then an empty referenced-column tuple is rejected
    with pytest.raises(ValueError, match="referenced_columns must not be empty"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=(),
            constraint_name="x_fk",
        )


def test_rejects_mismatched_column_counts():
    # Given local and referenced column tuples of different lengths
    # When / Then construction is rejected
    with pytest.raises(ValueError, match="same number of entries"):
        ForeignKeyConstraint(
            local_columns=("a", "b"),
            referenced_table=_customers(),
            referenced_columns=("id",),
            constraint_name="x_fk",
        )


def test_rejects_duplicate_local_columns():
    # Given / When / Then a repeated local column is rejected
    with pytest.raises(ValueError, match="Duplicate foreign key local column"):
        ForeignKeyConstraint(
            local_columns=("customer_id", "customer_id"),
            referenced_table=_customers(),
            referenced_columns=("tenant_id", "id"),
            constraint_name="x_fk",
        )


def test_rejects_duplicate_referenced_columns():
    # Given / When / Then a repeated referenced column is rejected
    with pytest.raises(ValueError, match="Duplicate foreign key referenced column"):
        ForeignKeyConstraint(
            local_columns=("customer_id", "tenant_id"),
            referenced_table=_customers(),
            referenced_columns=("id", "id"),
            constraint_name="x_fk",
        )


def test_rejects_blank_explicit_constraint_name():
    # Given / When / Then a blank explicit constraint name is rejected
    with pytest.raises(ValueError, match="constraint_name must not be blank"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=("id",),
            constraint_name="   ",
        )


def test_foreign_key_constraint_is_frozen():
    # Given a constraint
    constraint = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
        constraint_name="orders_customer_id_fk",
    )

    # When / Then assignment is rejected (frozen dataclass)
    with pytest.raises(AttributeError):
        constraint.referenced_table = _customers()  # type: ignore[misc]


def test_generate_names_constraint_from_table_and_local_columns():
    # Given a table name and foreign key content
    # When the engine generates the constraint
    constraint = ForeignKeyConstraint.generate(
        owner_table_name="orders",
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
    )

    # Then the name follows {table}_{local_cols}_fk
    assert constraint.constraint_name == "orders_customer_id_fk"


def test_construction_canonicalizes_pair_order_by_local_column():
    # Given pairs declared in non-canonical order: b->y, a->x
    constraint = ForeignKeyConstraint(
        local_columns=("b", "a"),
        referenced_table=_customers(),
        referenced_columns=("y", "x"),
        constraint_name="orders_fk",
    )

    # Then storage is sorted by local column with the pairing preserved
    assert constraint.local_columns == ("a", "b")
    assert constraint.referenced_columns == ("x", "y")


def test_signature_ignores_declared_pair_order():
    # Given the same relationship declared in two pair orders
    one = ForeignKeyConstraint(
        local_columns=("a", "b"),
        referenced_table=_customers(),
        referenced_columns=("x", "y"),
        constraint_name="orders_fk",
    )
    two = ForeignKeyConstraint(
        local_columns=("b", "a"),
        referenced_table=_customers(),
        referenced_columns=("y", "x"),
        constraint_name="orders_fk",
    )

    # Then they are the same constraint — order is not part of identity
    assert one.signature == two.signature


def test_generate_names_identically_for_permuted_pairs():
    # Given the same relationship generated from two pair orders
    one = ForeignKeyConstraint.generate(
        owner_table_name="orders",
        local_columns=("a", "b"),
        referenced_table=_customers(),
        referenced_columns=("x", "y"),
    )
    two = ForeignKeyConstraint.generate(
        owner_table_name="orders",
        local_columns=("b", "a"),
        referenced_table=_customers(),
        referenced_columns=("y", "x"),
    )

    # Then the generated name is order-independent and canonical
    assert one.constraint_name == "orders_a_b_fk"
    assert two.constraint_name == "orders_a_b_fk"


def test_mixed_case_columns_and_name_normalize_to_lowercase():
    fk = ForeignKeyConstraint(
        local_columns=("CustomerId",),
        referenced_table=QualifiedName("cat", "sales", "customers"),
        referenced_columns=("Id",),
        constraint_name="Orders_CustomerId_FK",
    )
    assert fk.local_columns == ("customerid",)
    assert fk.referenced_columns == ("id",)
    assert fk.constraint_name == "orders_customerid_fk"
