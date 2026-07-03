import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint


def _customers() -> QualifiedName:
    return QualifiedName("main", "sales", "customers")


def test_signature_ignores_constraint_name():
    # Given two FKs with identical content but different explicit names
    unnamed = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
    )
    named = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
        constraint_name="chosen_elsewhere",
    )

    # Then their signatures are equal — name is not part of content identity
    assert unnamed.signature == named.signature


def test_signature_differs_when_referenced_table_differs():
    # Given two FKs that differ only in the referenced table
    to_old = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("main", "sales", "old_customers"),
        referenced_columns=("id",),
    )
    to_new = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("main", "sales", "new_customers"),
        referenced_columns=("id",),
    )

    # Then their signatures differ
    assert to_old.signature != to_new.signature


def test_rejects_empty_local_columns():
    # Given / When / Then an empty local-column tuple is rejected
    with pytest.raises(ValueError, match="local_columns must not be empty"):
        ForeignKeyConstraint(
            local_columns=(),
            referenced_table=_customers(),
            referenced_columns=("id",),
        )


def test_rejects_empty_referenced_columns():
    # Given / When / Then an empty referenced-column tuple is rejected
    with pytest.raises(ValueError, match="referenced_columns must not be empty"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=(),
        )


def test_rejects_mismatched_column_counts():
    # Given local and referenced column tuples of different lengths
    # When / Then construction is rejected
    with pytest.raises(ValueError, match="same number of entries"):
        ForeignKeyConstraint(
            local_columns=("a", "b"),
            referenced_table=_customers(),
            referenced_columns=("id",),
        )


def test_rejects_blank_explicit_constraint_name():
    # Given / When / Then a blank explicit constraint name is rejected
    with pytest.raises(ValueError, match="constraint_name must not be blank"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=_customers(),
            referenced_columns=("id",),
            constraint_name="   ",
        )


def test_generated_name_follows_table_and_local_columns():
    # Given an unnamed desired constraint
    constraint = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
    )

    # When the engine generates its name from the owning table
    named = constraint.with_generated_name("orders")

    # Then the name follows {table}_{local_cols}_fk
    assert named.constraint_name == "orders_customer_id_fk"


def test_foreign_key_constraint_is_frozen():
    # Given a constraint
    constraint = ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=_customers(),
        referenced_columns=("id",),
    )

    # When / Then assignment is rejected (frozen dataclass)
    with pytest.raises(AttributeError):
        constraint.referenced_table = _customers()  # type: ignore[misc]
