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


def test_signature_is_identical_across_declaration_casing():
    # Given equivalent columns with different display casing
    camel = PrimaryKeyConstraint(columns=("RequestId",), constraint_name="t_pk")
    lower = PrimaryKeyConstraint(columns=("requestid",), constraint_name="t_pk")

    # Then their content identity is the same
    assert camel.signature == lower.signature


def test_rejects_columns_differing_only_by_case_as_duplicates():
    # Given / When / Then two spellings of one identifier are rejected as duplicates
    with pytest.raises(ValueError):
        PrimaryKeyConstraint(columns=("id", "ID"), constraint_name="t_pk")
