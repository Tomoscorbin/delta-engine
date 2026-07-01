import pytest

from delta_engine.domain.model.primary_key import PrimaryKeyConstraint


def test_resolves_default_constraint_name_from_table_name():
    # Given a primary key with no explicit constraint name
    primary_key = PrimaryKeyConstraint(columns=("id",))

    # Then the resolved name is derived from the table name
    assert primary_key.resolve_constraint_name("customers") == "customers_pk"


def test_uses_explicit_constraint_name_when_provided():
    # Given a primary key with an explicit name
    primary_key = PrimaryKeyConstraint(columns=("id",), constraint_name="custom_pk")

    # Then the explicit name wins
    assert primary_key.resolve_constraint_name("customers") == "custom_pk"


def test_rejects_empty_columns():
    # Given / Then constructing with no columns is an error
    with pytest.raises(ValueError, match="columns must not be empty"):
        PrimaryKeyConstraint(columns=())


def test_rejects_duplicate_columns():
    # Given / Then a repeated column is an error
    with pytest.raises(ValueError, match=r"[Dd]uplicate"):
        PrimaryKeyConstraint(columns=("id", "id"))


def test_rejects_blank_explicit_constraint_name():
    # Given / Then a blank explicit name is an error
    with pytest.raises(ValueError, match="constraint_name must not be blank"):
        PrimaryKeyConstraint(columns=("id",), constraint_name="  ")


def test_equal_by_value():
    # Given two primary keys with the same columns and name
    # Then they compare equal (frozen value object)
    assert PrimaryKeyConstraint(columns=("a", "b")) == PrimaryKeyConstraint(columns=("a", "b"))
