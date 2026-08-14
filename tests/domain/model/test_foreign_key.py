import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.constraints import (
    DesiredForeignKey,
    DesiredPrimaryKey,
    ObservedForeignKey,
    ObservedReferencingForeignKey,
)


def _customers() -> QualifiedName:
    return QualifiedName("main", "sales", "customers")


def test_constraint_identity_excludes_lifecycle_names():
    # Given desired and observed FKs with identical definitions but different names
    one = DesiredForeignKey(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
        desired_name="orders_customer_id_fk",
    )
    two = ObservedForeignKey(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
        catalog_name="chosen_elsewhere",
    )

    # Then direct equality expresses relational equivalence in both directions
    assert one == two
    assert two == one
    assert hash(one) == hash(two)
    assert two.catalog_name == "chosen_elsewhere"
    assert type(two.catalog_name) is str


def test_constraint_identity_includes_the_referenced_table():
    # Given two FKs that differ only in the referenced table
    to_old = DesiredForeignKey(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("main", "sales", "old_customers"),
        referenced_columns=("id",),
        desired_name="orders_customer_id_fk",
    )
    to_new = DesiredForeignKey(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("main", "sales", "new_customers"),
        referenced_columns=("id",),
        desired_name="orders_customer_id_fk",
    )

    # Then they are different managed constraints
    assert to_old != to_new


def test_rejects_empty_local_columns():
    # When a foreign key has no local columns, then construction fails
    with pytest.raises(ValueError):
        DesiredForeignKey(
            local_columns=(),
            referenced_table=_customers(),
            referenced_columns=("id",),
            desired_name="x_fk",
        )


def test_rejects_empty_referenced_columns():
    # When a foreign key has no referenced columns, then construction fails
    with pytest.raises(ValueError):
        DesiredForeignKey(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=(),
            desired_name="x_fk",
        )


@pytest.mark.parametrize(
    ("local_columns", "referenced_columns"),
    [
        pytest.param("customer_id", ("id",), id="bare-local-string"),
        pytest.param(("customer_id",), "id", id="bare-referenced-string"),
        pytest.param(("customer_id", 42), ("tenant_id", "id"), id="non-string-local"),
        pytest.param(("customer_id", "tenant_id"), ("id", 42), id="non-string-referenced"),
    ],
)
def test_rejects_invalid_column_collections(
    local_columns: object,
    referenced_columns: object,
) -> None:
    # When malformed columns are supplied, then construction fails at the value boundary
    with pytest.raises(TypeError):
        DesiredForeignKey(
            local_columns=local_columns,  # type: ignore[arg-type]
            referenced_table=_customers(),
            referenced_columns=referenced_columns,  # type: ignore[arg-type]
            desired_name="x_fk",
        )


def test_rejects_mismatched_column_counts():
    # Given local and referenced column tuples of different lengths
    # When the foreign key is constructed, then the mismatch is rejected
    with pytest.raises(ValueError):
        DesiredForeignKey(
            local_columns=("a", "b"),
            referenced_table=_customers(),
            referenced_columns=("id",),
            desired_name="x_fk",
        )


def test_rejects_duplicate_local_columns():
    # When a foreign key repeats a local column, then construction fails
    with pytest.raises(ValueError):
        DesiredForeignKey(
            local_columns=("customer_id", "customer_id"),
            referenced_table=_customers(),
            referenced_columns=("tenant_id", "id"),
            desired_name="x_fk",
        )


def test_rejects_duplicate_referenced_columns():
    # When a foreign key repeats a referenced column, then construction fails
    with pytest.raises(ValueError):
        DesiredForeignKey(
            local_columns=("customer_id", "tenant_id"),
            referenced_table=_customers(),
            referenced_columns=("id", "id"),
            desired_name="x_fk",
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
    # When the physical name is invalid, then construction fails deliberately
    with pytest.raises(expected_error):
        DesiredForeignKey(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=("id",),
            desired_name=constraint_name,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("catalog_name", "expected_error"),
    [
        pytest.param("   ", ValueError, id="blank"),
        pytest.param(42, TypeError, id="not-a-string"),
    ],
)
def test_observed_foreign_key_rejects_invalid_catalog_name(
    catalog_name: object,
    expected_error: type[Exception],
) -> None:
    with pytest.raises(expected_error):
        ObservedForeignKey(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=("id",),
            catalog_name=catalog_name,  # type: ignore[arg-type]
        )


def test_foreign_key_reference_rejects_non_string_constraint_name() -> None:
    # When the physical name is not a string, then construction fails deliberately
    with pytest.raises(TypeError):
        ObservedReferencingForeignKey(
            catalog_name=42,  # type: ignore[arg-type]
            referencing_table=_customers(),
        )


def test_foreign_key_reference_retains_exact_catalog_name() -> None:
    reference = ObservedReferencingForeignKey(
        catalog_name="Orders_Customer_FK",
        referencing_table=_customers(),
    )

    assert reference.catalog_name == "Orders_Customer_FK"
    assert type(reference.catalog_name) is str


def test_construction_canonicalizes_pair_order_by_local_column():
    # Given pairs declared in non-canonical order: b->y, a->x
    constraint = DesiredForeignKey(
        local_columns=("b", "a"),
        referenced_table=_customers(),
        referenced_columns=("y", "x"),
        desired_name="orders_fk",
    )

    # Then storage is sorted by local column with the pairing preserved
    assert constraint.local_columns == ("a", "b")
    assert constraint.referenced_columns == ("x", "y")


def test_constraint_identity_ignores_declared_pair_order():
    # Given the same relationship declared in two pair orders
    one = DesiredForeignKey(
        local_columns=("a", "b"),
        referenced_table=_customers(),
        referenced_columns=("x", "y"),
        desired_name="orders_fk",
    )
    two = DesiredForeignKey(
        local_columns=("b", "a"),
        referenced_table=_customers(),
        referenced_columns=("y", "x"),
        desired_name="orders_fk",
    )

    # Then they are the same constraint — order is not part of identity
    assert one == two


def test_mixed_case_columns_are_preserved_and_sorted_by_identity():
    constraint = DesiredForeignKey(
        local_columns=("Zebra", "Apple"),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("z_id", "a_id"),
        desired_name="t_fk",
    )
    assert tuple(str(column) for column in constraint.local_columns) == ("Apple", "Zebra")
    assert tuple(str(column) for column in constraint.referenced_columns) == ("a_id", "z_id")


def test_rejects_local_columns_differing_only_by_case_as_duplicates():
    with pytest.raises(ValueError):
        DesiredForeignKey(
            local_columns=("id", "ID"),
            referenced_table=QualifiedName("cat", "sch", "parent"),
            referenced_columns=("a", "b"),
            desired_name="t_fk",
        )


def test_constraints_match_across_case_variant_spellings() -> None:
    # Given the same constraint under different column and name casing
    declared = DesiredForeignKey(
        local_columns=("orderref",),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("orderid",),
        desired_name="child_orderref_fk",
    )
    observed = ObservedForeignKey(
        local_columns=("OrderRef",),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("OrderId",),
        catalog_name="CATALOG_GENERATED_FK",
    )

    # Then identity is case-insensitive while each spelling is preserved
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
    declared = DesiredForeignKey(
        local_columns=("Zeta", "alpha"),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("z_id", "a_id"),
        desired_name="t_fk",
    )
    observed = ObservedForeignKey(
        local_columns=("ZETA", "ALPHA"),
        referenced_table=QualifiedName("cat", "sch", "parent"),
        referenced_columns=("Z_ID", "A_ID"),
        catalog_name="T_FK",
    )

    # Then they are recognized as the same constraint — canonicalization is
    # identity-keyed, not raw-string sorted
    assert declared == observed


def test_primary_and_foreign_keys_never_compare_equal() -> None:
    primary_key = DesiredPrimaryKey(columns=("customer_id",), desired_name="orders_pk")
    foreign_key = DesiredForeignKey(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
        desired_name="orders_fk",
    )

    assert primary_key != foreign_key
    assert foreign_key != primary_key
