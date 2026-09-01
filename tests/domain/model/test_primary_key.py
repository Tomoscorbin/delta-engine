import pytest

from delta_engine.domain.model.constraints import (
    ObservedPrimaryKeyConstraint,
    PrimaryKeyConstraint,
)


def test_rejects_empty_columns():
    # When a primary key has no columns, then construction fails
    with pytest.raises(ValueError):
        PrimaryKeyConstraint(columns=(), name="t_pk")


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param("id", id="bare-string"),
        pytest.param(("id", 42), id="non-string-entry"),
    ],
)
def test_rejects_invalid_column_collections(columns: object) -> None:
    # When malformed columns are supplied, then construction fails at the value boundary
    with pytest.raises(TypeError):
        PrimaryKeyConstraint(
            columns=columns,  # type: ignore[arg-type]
            name="t_pk",
        )


def test_rejects_duplicate_columns():
    # When a primary key repeats a column, then construction fails
    with pytest.raises(ValueError):
        PrimaryKeyConstraint(columns=("id", "id"), name="t_pk")


def test_constraint_name_may_be_omitted():
    key = PrimaryKeyConstraint(columns=("id",))

    assert key.name is None


@pytest.mark.parametrize(
    ("invalid_name", "expected_error"),
    [
        pytest.param("  ", ValueError, id="blank"),
        pytest.param(42, TypeError, id="not-a-string"),
    ],
)
def test_rejects_invalid_constraint_name(
    invalid_name: object,
    expected_error: type[Exception],
):
    # When the physical name is invalid, then construction fails deliberately
    with pytest.raises(expected_error):
        PrimaryKeyConstraint(
            columns=("id",),
            name=invalid_name,  # type: ignore[arg-type]
        )


def test_observed_constraint_requires_a_name():
    with pytest.raises(TypeError, match="string"):
        ObservedPrimaryKeyConstraint(columns=("id",), name=None)  # type: ignore[arg-type]


def test_mixed_case_columns_and_name_are_preserved():
    # Given a constraint with mixed-case display spelling
    pk = PrimaryKeyConstraint(columns=("OrderId",), name="Orders_PK")

    # Then construction preserves that spelling
    assert tuple(str(column) for column in pk.columns) == ("OrderId",)
    assert str(pk.name) == "Orders_PK"


def test_construction_canonicalizes_column_order():
    # Given columns declared in non-canonical order
    key = PrimaryKeyConstraint(columns=("b", "a"), name="t_pk")

    # Then storage is sorted by identifier key
    assert key.columns == ("a", "b")


def test_mixed_case_columns_are_preserved_and_sorted_by_identity():
    # Given mixed-case columns whose identity order differs from declaration order
    key = PrimaryKeyConstraint(columns=("Zebra", "Apple"), name="t_pk")

    # Then each spelling is preserved and storage is sorted by identifier key
    assert tuple(str(column) for column in key.columns) == ("Apple", "Zebra")


def test_canonical_order_is_identity_keyed_not_raw_string_sorted():
    # Given two declarations whose raw ASCII order disagrees with identity order:
    # "Zeta" precedes "alpha" by raw byte value (Z < a), but "zeta" follows
    # "alpha" by identifier key. Sorting on the bare spelling would canonicalize
    # the two declarations into different orders and break their equality.
    declared = PrimaryKeyConstraint(columns=("Zeta", "alpha"), name="t_pk")
    observed = PrimaryKeyConstraint(columns=("ZETA", "ALPHA"), name="T_PK")

    # Then both canonicalize to the same identity-keyed order
    assert declared == observed
    assert hash(declared) == hash(observed)


def test_equality_uses_structural_column_set_identity():
    # Given equivalent constraints with different names, casing, and column order
    desired = PrimaryKeyConstraint(columns=("TenantId", "OrderId"), name="Orders_PK")
    observed = ObservedPrimaryKeyConstraint(
        columns=("orderid", "tenantid"),
        name="legacy_pk",
    )

    # When comparing the constraints
    are_equal = desired == observed

    # Then only their semantic column set determines equality
    assert are_equal
    assert hash(desired) == hash(observed)


def test_equality_rejects_a_different_column_set():
    key = PrimaryKeyConstraint(columns=("id",), name="orders_pk")
    other = PrimaryKeyConstraint(columns=("other_id",), name="orders_pk")

    assert key != other


def test_name_is_a_creation_preference_not_structural_identity():
    unnamed = PrimaryKeyConstraint(columns=("id",))
    named = PrimaryKeyConstraint(columns=("id",), name="orders_pk")

    assert unnamed == named
    assert hash(unnamed) == hash(named)
    assert unnamed == PrimaryKeyConstraint(columns=("ID",))
    assert hash(unnamed) == hash(PrimaryKeyConstraint(columns=("ID",)))


def test_matches_columns_excludes_the_constraint_name_from_comparison():
    # Given a named, composite primary key
    key = PrimaryKeyConstraint(columns=("TenantId", "OrderId"), name="orders_pk")

    # When comparing columns with different order and identifier casing
    matches = key.matches_columns(("orderid", "tenantid"))

    # Then only the semantic column set is considered
    assert matches
    assert not key.matches_columns(("orderid", "customerid"))


def test_rejects_columns_differing_only_by_case_as_duplicates():
    # When one column is repeated with different casing, then construction fails
    with pytest.raises(ValueError):
        PrimaryKeyConstraint(columns=("id", "ID"), name="t_pk")
