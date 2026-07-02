import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint


def test_signature_ignores_constraint_name():
    # Given two FKs with identical content but different explicit names
    unnamed = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references=QualifiedName("main", "sales", "customers"),
        referenced_columns=("id",),
    )
    named = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references=QualifiedName("main", "sales", "customers"),
        referenced_columns=("id",),
        constraint_name="chosen_elsewhere",
    )

    # Then their signatures are equal — name is not part of content identity
    assert unnamed.signature == named.signature


def test_signature_differs_when_referenced_table_differs():
    # Given two FKs that differ only in the referenced table
    to_old = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references=QualifiedName("main", "sales", "old_customers"),
        referenced_columns=("id",),
    )
    to_new = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references=QualifiedName("main", "sales", "new_customers"),
        referenced_columns=("id",),
    )

    # Then their signatures differ
    assert to_old.signature != to_new.signature


def test_rejects_empty_local_columns():
    # Given / When / Then
    with pytest.raises(ValueError, match="local_columns"):
        ForeignKeyConstraint(
            local_columns=(),
            references=QualifiedName("main", "sales", "customers"),
            referenced_columns=("id",),
        )


def test_rejects_empty_referenced_columns():
    with pytest.raises(ValueError, match="referenced_columns"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            references=QualifiedName("main", "sales", "customers"),
            referenced_columns=(),
        )


def test_rejects_mismatched_column_counts():
    with pytest.raises(ValueError, match="must have the same number"):
        ForeignKeyConstraint(
            local_columns=("a", "b"),
            references=QualifiedName("main", "sales", "customers"),
            referenced_columns=("id",),
        )


def test_rejects_blank_explicit_constraint_name():
    # Given an explicit but blank constraint name
    # When / Then — a blank name would compile to invalid `ADD CONSTRAINT `` ...`
    with pytest.raises(ValueError, match="constraint_name"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            references=QualifiedName("main", "sales", "customers"),
            referenced_columns=("id",),
            constraint_name="   ",
        )


def test_foreign_key_constraint_is_frozen():
    # Given
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references=QualifiedName("main", "sales", "customers"),
        referenced_columns=("id",),
    )

    # When / Then — frozen dataclass raises on mutation
    with pytest.raises(AttributeError):
        fk.references = QualifiedName("main", "sales", "other")  # type: ignore[misc]
