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
