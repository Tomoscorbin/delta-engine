import pytest

from delta_engine.domain.model.constraints import PrimaryKeyConstraint


def test_rejects_empty_columns():
    # Given / Then constructing with no columns is an error
    with pytest.raises(ValueError):
        PrimaryKeyConstraint(columns=(), constraint_name="t_pk")


def test_rejects_duplicate_columns():
    # Given / Then a repeated column is an error
    with pytest.raises(ValueError):
        PrimaryKeyConstraint(columns=("id", "id"), constraint_name="t_pk")


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
    # Given / When / Then an invalid physical name is rejected
    with pytest.raises(expected_error):
        PrimaryKeyConstraint(
            columns=("id",),
            constraint_name=invalid_name,  # type: ignore[arg-type]
        )


def test_mixed_case_columns_and_name_are_preserved():
    # Given a constraint with mixed-case display spelling
    pk = PrimaryKeyConstraint(columns=("OrderId",), constraint_name="Orders_PK")

    # Then construction preserves that spelling
    assert tuple(str(column) for column in pk.columns) == ("OrderId",)
    assert str(pk.constraint_name) == "Orders_PK"


def test_equality_uses_constraint_name_and_column_set_identity():
    # Given equivalent constraints with different identifier casing and column order
    desired = PrimaryKeyConstraint(columns=("TenantId", "OrderId"), constraint_name="Orders_PK")
    observed = PrimaryKeyConstraint(columns=("orderid", "tenantid"), constraint_name="orders_pk")

    # When comparing the constraints
    are_equal = desired == observed

    # Then their physical name and semantic column set make them the same value
    assert are_equal
    assert hash(desired) == hash(observed)


@pytest.mark.parametrize(
    "other",
    [
        pytest.param(
            PrimaryKeyConstraint(columns=("id",), constraint_name="legacy_pk"),
            id="different-name",
        ),
        pytest.param(
            PrimaryKeyConstraint(columns=("other_id",), constraint_name="orders_pk"),
            id="different-columns",
        ),
    ],
)
def test_equality_rejects_different_managed_identity(other: PrimaryKeyConstraint):
    # Given a primary key with either a different name or column set
    key = PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk")

    # When comparing the constraints
    are_equal = key == other

    # Then they are different managed values
    assert not are_equal


def test_matches_columns_excludes_the_constraint_name_from_comparison():
    # Given a named, composite primary key
    key = PrimaryKeyConstraint(columns=("TenantId", "OrderId"), constraint_name="orders_pk")

    # When comparing columns with different order and identifier casing
    matches = key.matches_columns(("orderid", "tenantid"))

    # Then only the semantic column set is considered
    assert matches
    assert not key.matches_columns(("orderid", "customerid"))


def test_rejects_columns_differing_only_by_case_as_duplicates():
    # Given / When / Then two spellings of one identifier are rejected as duplicates
    with pytest.raises(ValueError):
        PrimaryKeyConstraint(columns=("id", "ID"), constraint_name="t_pk")
