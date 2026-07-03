import pytest

from delta_engine.domain.model.primary_key import PrimaryKeyConstraint


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


def test_generate_names_constraint_from_table():
    # Given a table name and key columns
    # When the engine generates the constraint
    constraint = PrimaryKeyConstraint.generate(table_name="orders", columns=("id",))

    # Then the name follows {table}_pk and the columns are carried through
    assert constraint.constraint_name == "orders_pk"
    assert constraint.columns == ("id",)
