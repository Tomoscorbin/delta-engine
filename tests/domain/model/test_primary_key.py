import pytest

from delta_engine.domain.model import key_signature
from delta_engine.domain.model.constraints import PrimaryKeyConstraint


def test_rejects_empty_columns():
    # Given / Then constructing with no columns is an error
    with pytest.raises(ValueError, match="columns must not be empty"):
        PrimaryKeyConstraint(columns=(), constraint_name="t_pk")


def test_rejects_duplicate_columns():
    # Given / Then a repeated column is an error
    with pytest.raises(ValueError, match=r"[Dd]uplicate"):
        PrimaryKeyConstraint(columns=("id", "id"), constraint_name="t_pk")


def test_rejects_blank_explicit_constraint_name():
    # Given / Then a blank explicit name is an error
    with pytest.raises(ValueError, match="constraint_name must not be blank"):
        PrimaryKeyConstraint(columns=("id",), constraint_name="  ")


def test_rejects_non_string_explicit_constraint_name():
    with pytest.raises(TypeError, match="constraint_name must be a string or None"):
        PrimaryKeyConstraint(columns=("id",), constraint_name=42)  # type: ignore[arg-type]


def test_mixed_case_columns_and_name_are_preserved():
    pk = PrimaryKeyConstraint(columns=("OrderId",), constraint_name="Orders_PK")
    assert tuple(str(column) for column in pk.columns) == ("OrderId",)
    assert str(pk.constraint_name) == "Orders_PK"


def test_signature_is_identical_across_declaration_casing():
    camel = PrimaryKeyConstraint(columns=("RequestId",), constraint_name="t_pk")
    lower = PrimaryKeyConstraint(columns=("requestid",), constraint_name="t_pk")

    assert camel.signature == lower.signature
    assert key_signature(("RequestId",)) == key_signature(("requestid",))


def test_rejects_columns_differing_only_by_case_as_duplicates():
    with pytest.raises(ValueError, match=r"[Dd]uplicate"):
        PrimaryKeyConstraint(columns=("id", "ID"), constraint_name="t_pk")


def test_unpinned_key_adopts_a_semantically_matching_observed_name():
    desired = PrimaryKeyConstraint(columns=("a", "b"))
    observed = PrimaryKeyConstraint(columns=("B", "A"), constraint_name="legacy_pk")

    assert desired.is_satisfied_by(observed)


def test_explicit_name_is_managed_by_identifier_identity():
    desired = PrimaryKeyConstraint(columns=("id",), constraint_name="Orders_PK")

    assert desired.is_satisfied_by(
        PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk")
    )
    assert not desired.is_satisfied_by(
        PrimaryKeyConstraint(columns=("id",), constraint_name="legacy_pk")
    )


def test_resolved_name_uses_the_default_only_for_an_unpinned_key():
    assert PrimaryKeyConstraint(("id",)).resolved_name("Orders") == "Orders_pk"
    assert PrimaryKeyConstraint(("id",), "business_key").resolved_name("Orders") == "business_key"
