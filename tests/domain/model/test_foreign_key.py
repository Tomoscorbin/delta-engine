import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.constraints import ForeignKeyConstraint


def _customers() -> QualifiedName:
    return QualifiedName("main", "sales", "customers")


def test_constraint_identity_includes_the_physical_name():
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

    # Then they are different managed constraints
    assert one != two


def test_constraint_identity_includes_the_referenced_table():
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

    # Then they are different managed constraints
    assert to_old != to_new


def test_rejects_empty_local_columns():
    # Given / When / Then an empty local-column tuple is rejected
    with pytest.raises(ValueError):
        ForeignKeyConstraint(
            local_columns=(),
            referenced_table=_customers(),
            referenced_columns=("id",),
            constraint_name="x_fk",
        )


def test_rejects_empty_referenced_columns():
    # Given / When / Then an empty referenced-column tuple is rejected
    with pytest.raises(ValueError):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=(),
            constraint_name="x_fk",
        )


def test_rejects_mismatched_column_counts():
    # Given local and referenced column tuples of different lengths
    # When / Then construction is rejected
    with pytest.raises(ValueError):
        ForeignKeyConstraint(
            local_columns=("a", "b"),
            referenced_table=_customers(),
            referenced_columns=("id",),
            constraint_name="x_fk",
        )


def test_rejects_duplicate_local_columns():
    # Given / When / Then a repeated local column is rejected
    with pytest.raises(ValueError):
        ForeignKeyConstraint(
            local_columns=("customer_id", "customer_id"),
            referenced_table=_customers(),
            referenced_columns=("tenant_id", "id"),
            constraint_name="x_fk",
        )


def test_rejects_duplicate_referenced_columns():
    # Given / When / Then a repeated referenced column is rejected
    with pytest.raises(ValueError):
        ForeignKeyConstraint(
            local_columns=("customer_id", "tenant_id"),
            referenced_table=_customers(),
            referenced_columns=("id", "id"),
            constraint_name="x_fk",
        )


@pytest.mark.parametrize(
    ("constraint_name", "expected_error"),
    [
        pytest.param("   ", ValueError, id="blank"),
        pytest.param(42, TypeError, id="not-a-string"),
    ],
)
def test_rejects_invalid_constraint_name(
    constraint_name: object,
    expected_error: type[Exception],
):
    # Given / When / Then an invalid physical name is rejected
    with pytest.raises(expected_error):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=("id",),
            constraint_name=constraint_name,  # type: ignore[arg-type]
        )


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


def test_constraint_identity_ignores_declared_pair_order():
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
    assert one == two


def test_mixed_case_columns_are_preserved_and_sorted_by_identity():
    constraint = ForeignKeyConstraint(
        local_columns=("Zebra", "Apple"),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("z_id", "a_id"),
        constraint_name="t_fk",
    )
    assert tuple(str(column) for column in constraint.local_columns) == ("Apple", "Zebra")
    assert tuple(str(column) for column in constraint.referenced_columns) == ("a_id", "z_id")


def test_constraint_identity_is_case_insensitive():
    referenced = QualifiedName("cat", "sch", "parent")
    camel = ForeignKeyConstraint(
        local_columns=("OrderId",),
        referenced_table=referenced,
        referenced_columns=("Id",),
        constraint_name="t_orderid_fk",
    )
    lower = ForeignKeyConstraint(
        local_columns=("orderid",),
        referenced_table=referenced,
        referenced_columns=("id",),
        constraint_name="t_orderid_fk",
    )

    assert camel == lower


def test_rejects_local_columns_differing_only_by_case_as_duplicates():
    with pytest.raises(ValueError):
        ForeignKeyConstraint(
            local_columns=("id", "ID"),
            referenced_table=QualifiedName("cat", "sch", "parent"),
            referenced_columns=("a", "b"),
            constraint_name="t_fk",
        )


def test_constraints_match_across_case_variant_spellings() -> None:
    declared = ForeignKeyConstraint(
        local_columns=("orderref",),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("orderid",),
        constraint_name="child_orderref_fk",
    )
    observed = ForeignKeyConstraint(
        local_columns=("OrderRef",),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("OrderId",),
        constraint_name="CHILD_ORDERREF_FK",
    )
    assert declared == observed
    assert str(declared.local_columns[0]) == "orderref"
    assert str(observed.local_columns[0]) == "OrderRef"


def test_constraint_identity_matches_when_case_flips_raw_pair_sort_order() -> None:
    # Given the same two-pair relationship declared with a case pattern
    # where raw (case-sensitive) ordering of the sort column disagrees with
    # identity-key ordering: "Zeta" sorts before "alpha" by raw ASCII (Z <
    # a), but "zeta" sorts after "alpha" by identity key. Sorting pairs by
    # the bare case-sensitive spelling instead of the lowercased identity
    # would canonicalize the two declarations into different pair orders.
    declared = ForeignKeyConstraint(
        local_columns=("Zeta", "alpha"),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("z_id", "a_id"),
        constraint_name="t_fk",
    )
    observed = ForeignKeyConstraint(
        local_columns=("ZETA", "ALPHA"),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("Z_ID", "A_ID"),
        constraint_name="T_FK",
    )

    # Then they are recognized as the same constraint — canonicalization is
    # identity-keyed, not raw-string sorted
    assert declared == observed
