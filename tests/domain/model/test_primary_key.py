import pytest

from delta_engine.domain.model.constraints import DesiredPrimaryKey, ObservedPrimaryKey


def test_rejects_empty_columns():
    # When a primary key has no columns, then construction fails
    with pytest.raises(ValueError):
        DesiredPrimaryKey(columns=(), requested_name="t_pk")


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
        DesiredPrimaryKey(
            columns=columns,  # type: ignore[arg-type]
            requested_name="t_pk",
        )


def test_rejects_duplicate_columns():
    # When a primary key repeats a column, then construction fails
    with pytest.raises(ValueError):
        DesiredPrimaryKey(columns=("id", "id"), requested_name="t_pk")


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
        DesiredPrimaryKey(
            columns=("id",),
            requested_name=invalid_name,  # type: ignore[arg-type]
        )


def test_mixed_case_columns_and_name_are_preserved():
    # Given a constraint with mixed-case display spelling
    pk = DesiredPrimaryKey(columns=("OrderId",), requested_name="Orders_PK")

    # Then construction preserves that spelling
    assert tuple(str(column) for column in pk.columns) == ("OrderId",)
    assert str(pk.requested_name) == "Orders_PK"


def test_desired_and_observed_keys_compare_by_definition():
    # Given equivalent lifecycle values with different names, casing, and column order
    desired = DesiredPrimaryKey(columns=("TenantId", "OrderId"), requested_name="Orders_PK")
    observed = ObservedPrimaryKey(
        columns=("orderid", "tenantid"), catalog_name="catalog_generated_name"
    )

    # When comparing the constraints
    are_equal = desired == observed

    # Then the relational column set alone makes them the same value
    assert are_equal
    assert observed == desired
    assert hash(desired) == hash(observed)


@pytest.mark.parametrize(
    "other",
    [
        DesiredPrimaryKey(columns=("other_id",), requested_name="orders_pk"),
        ObservedPrimaryKey(columns=("other_id",), catalog_name="catalog_pk"),
    ],
)
def test_equality_rejects_different_definitions(
    other: DesiredPrimaryKey | ObservedPrimaryKey,
):
    # Given primary keys over different column sets
    key = DesiredPrimaryKey(columns=("id",), requested_name="orders_pk")

    # When comparing the constraints
    are_equal = key == other

    # Then they are different relational definitions
    assert not are_equal


def test_requested_name_is_not_part_of_desired_key_equality() -> None:
    one = DesiredPrimaryKey(columns=("id",), requested_name="one_pk")
    two = DesiredPrimaryKey(columns=("id",), requested_name="two_pk")

    assert one == two
    assert hash(one) == hash(two)


@pytest.mark.parametrize(
    ("catalog_name", "expected_error"),
    [
        pytest.param("  ", ValueError, id="blank"),
        pytest.param(42, TypeError, id="not-a-string"),
    ],
)
def test_observed_key_rejects_invalid_catalog_name(
    catalog_name: object,
    expected_error: type[Exception],
) -> None:
    with pytest.raises(expected_error):
        ObservedPrimaryKey(
            columns=("id",),
            catalog_name=catalog_name,  # type: ignore[arg-type]
        )


def test_observed_key_retains_exact_catalog_name_identity() -> None:
    upper = ObservedPrimaryKey(columns=("id",), catalog_name="Orders_PK")
    lower = ObservedPrimaryKey(columns=("id",), catalog_name="orders_pk")

    assert upper.catalog_name == "Orders_PK"
    assert type(upper.catalog_name) is str
    assert upper.catalog_name != lower.catalog_name


def test_matches_columns_excludes_the_constraint_name_from_comparison():
    # Given a named, composite primary key
    key = DesiredPrimaryKey(columns=("TenantId", "OrderId"), requested_name="orders_pk")

    # When comparing columns with different order and identifier casing
    matches = key.matches_columns(("orderid", "tenantid"))

    # Then only the semantic column set is considered
    assert matches
    assert not key.matches_columns(("orderid", "customerid"))


def test_rejects_columns_differing_only_by_case_as_duplicates():
    # When one column is repeated with different casing, then construction fails
    with pytest.raises(ValueError):
        DesiredPrimaryKey(columns=("id", "ID"), requested_name="t_pk")
