from collections.abc import Callable
from typing import Any

import pytest

from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.constraints import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
)

_CUSTOMERS = QualifiedName("main", "sales", "customers")


def _primary_key_over(columns: Any) -> PrimaryKeyConstraint:
    return PrimaryKeyConstraint(columns=columns, name="t_pk")


def _foreign_key_over(columns: Any) -> ForeignKeyConstraint:
    referenced = (
        tuple(f"parent_{index}" for index in range(len(columns)))
        if isinstance(columns, (list, tuple))
        else ("parent_0",)
    )
    return ForeignKeyConstraint(
        local_columns=columns,
        referenced_table=_CUSTOMERS,
        referenced_columns=referenced,
        name="t_fk",
    )


def _primary_key_named(name: Any) -> PrimaryKeyConstraint:
    return PrimaryKeyConstraint(columns=("id",), name=name)


def _foreign_key_named(name: Any) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=_CUSTOMERS,
        referenced_columns=("id",),
        name=name,
    )


def _foreign_key(
    local_columns: tuple[str, ...],
    referenced_columns: tuple[str, ...],
    name: str | None = "t_fk",
    referenced_table: QualifiedName = _CUSTOMERS,
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=local_columns,
        referenced_table=referenced_table,
        referenced_columns=referenced_columns,
        name=name,
    )


_EACH_CONSTRAINT_KIND = pytest.mark.parametrize(
    "make_constraint",
    [_primary_key_over, _foreign_key_over],
    ids=["primary-key", "foreign-key"],
)
_EACH_NAMED_CONSTRAINT_KIND = pytest.mark.parametrize(
    "make_constraint",
    [_primary_key_named, _foreign_key_named],
    ids=["primary-key", "foreign-key"],
)


# ---------- invariants shared by both constraint kinds


@_EACH_CONSTRAINT_KIND
def test_rejects_empty_columns(make_constraint: Callable[[Any], Any]) -> None:
    # When a key constraint has no columns, then construction fails
    with pytest.raises(ValueError):
        make_constraint(())


@_EACH_CONSTRAINT_KIND
@pytest.mark.parametrize(
    "columns",
    [
        pytest.param("id", id="bare-string"),
        pytest.param(("id", 42), id="non-string-entry"),
    ],
)
def test_rejects_invalid_column_collections(
    make_constraint: Callable[[Any], Any], columns: Any
) -> None:
    # When malformed columns are supplied, then construction fails at the value boundary
    with pytest.raises(TypeError):
        make_constraint(columns)


@_EACH_CONSTRAINT_KIND
@pytest.mark.parametrize(
    "columns",
    [
        pytest.param(("id", "id"), id="exact-duplicate"),
        pytest.param(("id", "ID"), id="case-variant-duplicate"),
    ],
)
def test_rejects_duplicate_columns(
    make_constraint: Callable[[Any], Any], columns: tuple[str, ...]
) -> None:
    # When a constraint repeats a column, exactly or as a case variant
    # (case is not identity), then construction fails
    with pytest.raises(ValueError):
        make_constraint(columns)


@_EACH_NAMED_CONSTRAINT_KIND
def test_constraint_name_may_be_omitted(make_constraint: Callable[[Any], Any]) -> None:
    # Given a constraint declared without a physical name
    constraint = make_constraint(None)

    # Then the name is absent, leaving naming to Databricks at creation
    assert constraint.name is None


@_EACH_NAMED_CONSTRAINT_KIND
@pytest.mark.parametrize(
    ("name", "expected_error"),
    [
        pytest.param("   ", ValueError, id="blank"),
        pytest.param(42, TypeError, id="not-a-string"),
    ],
)
def test_rejects_invalid_constraint_name(
    make_constraint: Callable[[Any], Any],
    name: Any,
    expected_error: type[Exception],
) -> None:
    # When the physical name is invalid, then construction fails deliberately
    with pytest.raises(expected_error):
        make_constraint(name)


# ---------- primary keys


def test_primary_key_name_is_a_creation_preference_not_structural_identity() -> None:
    # Given equivalent keys whose names are omitted, explicit, or differently cased
    unnamed = PrimaryKeyConstraint(columns=("id",))
    named = PrimaryKeyConstraint(columns=("id",), name="orders_pk")

    # Then they are the same structural constraint
    assert unnamed == named
    assert hash(unnamed) == hash(named)
    assert unnamed == PrimaryKeyConstraint(columns=("ID",))
    assert hash(unnamed) == hash(PrimaryKeyConstraint(columns=("ID",)))


def test_primary_key_equality_uses_structural_column_set_identity() -> None:
    # Given equivalent desired and observed constraints with different names,
    # casing, and column order
    desired = PrimaryKeyConstraint(columns=("TenantId", "OrderId"), name="Orders_PK")
    observed = PrimaryKeyConstraint(columns=("orderid", "tenantid"), name="legacy_pk")

    # Then only their semantic column set determines equality
    assert desired == observed
    assert hash(desired) == hash(observed)


def test_primary_key_equality_rejects_a_different_column_set() -> None:
    # Given keys over different columns under the same name
    key = PrimaryKeyConstraint(columns=("id",), name="orders_pk")
    other = PrimaryKeyConstraint(columns=("other_id",), name="orders_pk")

    # Then they are different constraints
    assert key != other


def test_primary_key_canonicalizes_column_order_preserving_spelling() -> None:
    # Given mixed-case columns declared in non-canonical order
    key = PrimaryKeyConstraint(columns=("Zebra", "Apple"), name="Orders_PK")

    # Then storage is sorted by identifier key with column and name spelling preserved
    assert tuple(str(column) for column in key.columns) == ("Apple", "Zebra")
    assert str(key.name) == "Orders_PK"


def test_primary_key_canonical_order_is_identity_keyed_not_raw_string_sorted() -> None:
    # Given two declarations whose raw ASCII order disagrees with identity order:
    # "Zeta" precedes "alpha" by raw byte value (Z < a), but "zeta" follows
    # "alpha" by identifier key. Sorting on the bare spelling would canonicalize
    # the two declarations into different orders and break their equality.
    declared = PrimaryKeyConstraint(columns=("Zeta", "alpha"), name="t_pk")
    observed = PrimaryKeyConstraint(columns=("ZETA", "ALPHA"), name="T_PK")

    # Then both canonicalize to the same identity-keyed order
    assert declared == observed
    assert hash(declared) == hash(observed)


def test_matches_columns_excludes_the_constraint_name_from_comparison() -> None:
    # Given a named, composite primary key
    key = PrimaryKeyConstraint(columns=("TenantId", "OrderId"), name="orders_pk")

    # When comparing columns with different order and identifier casing
    matches = key.matches_columns(("orderid", "tenantid"))

    # Then only the semantic column set is considered
    assert matches
    assert not key.matches_columns(("orderid", "customerid"))


# ---------- foreign keys


def test_foreign_key_name_is_a_creation_preference_not_structural_identity() -> None:
    # Given FKs with identical content under different names, or no name at all
    named_one = _foreign_key(("customer_id",), ("id",), name="orders_customer_id_fk")
    named_two = _foreign_key(("customer_id",), ("id",), name="chosen_elsewhere")
    unnamed = _foreign_key(("customer_id",), ("id",), name=None)

    # Then they are the same structural constraint
    assert named_one == named_two
    assert named_one == unnamed
    assert hash(named_one) == hash(named_two)
    assert hash(named_one) == hash(unnamed)


def test_desired_and_observed_foreign_keys_compare_by_definition() -> None:
    # Given the same relationship as a declaration and a catalog observation,
    # under different names, casing, and pair order
    desired = ForeignKeyConstraint(
        local_columns=("CustomerId", "TenantId"),
        referenced_table=_CUSTOMERS,
        referenced_columns=("Id", "TenantId"),
        name="requested_name",
    )
    observed = ForeignKeyConstraint(
        local_columns=("tenantid", "customerid"),
        referenced_table=_CUSTOMERS,
        referenced_columns=("tenantid", "id"),
        name="legacy_customer_fk",
    )

    # Then they are the same structural constraint
    assert desired == observed
    assert hash(desired) == hash(observed)


def test_foreign_key_identity_includes_the_referenced_table() -> None:
    # Given two FKs that differ only in the referenced table
    to_old = _foreign_key(
        ("customer_id",),
        ("id",),
        referenced_table=QualifiedName("main", "sales", "old_customers"),
    )
    to_new = _foreign_key(
        ("customer_id",),
        ("id",),
        referenced_table=QualifiedName("main", "sales", "new_customers"),
    )

    # Then they are different managed constraints
    assert to_old != to_new


def test_foreign_key_rejects_mismatched_column_counts() -> None:
    # Given local and referenced column tuples of different lengths
    # Then construction rejects the mismatch
    with pytest.raises(ValueError):
        _foreign_key(("a", "b"), ("id",))


def test_foreign_key_rejects_duplicate_referenced_columns() -> None:
    # When a foreign key repeats a referenced column, then construction fails
    with pytest.raises(ValueError):
        _foreign_key(("customer_id", "tenant_id"), ("id", "id"))


def test_foreign_key_canonicalizes_pair_order_by_local_column() -> None:
    # Given mixed-case pairs declared in non-canonical order: Zebra->z_id, Apple->a_id
    constraint = _foreign_key(("Zebra", "Apple"), ("z_id", "a_id"))

    # Then storage is sorted by local identifier key with pairing and spelling preserved
    assert tuple(str(column) for column in constraint.local_columns) == ("Apple", "Zebra")
    assert tuple(str(column) for column in constraint.referenced_columns) == ("a_id", "z_id")


def test_foreign_key_identity_ignores_declared_pair_order() -> None:
    # Given the same relationship declared in two pair orders
    one = _foreign_key(("a", "b"), ("x", "y"), name="orders_fk")
    two = _foreign_key(("b", "a"), ("y", "x"), name="orders_fk")

    # Then they are the same constraint — order is not part of identity
    assert one == two


def test_foreign_key_matches_across_case_variant_spellings() -> None:
    # Given the same constraint under different column and name casing
    declared = _foreign_key(("orderref",), ("orderid",), name="child_orderref_fk")
    observed = _foreign_key(("OrderRef",), ("OrderId",), name="CHILD_ORDERREF_FK")

    # Then identity is case-insensitive while each spelling is preserved
    assert declared == observed
    assert str(declared.local_columns[0]) == "orderref"
    assert str(observed.local_columns[0]) == "OrderRef"


def test_foreign_key_identity_matches_when_case_flips_raw_pair_sort_order() -> None:
    # Given the same two-pair relationship declared with a case pattern
    # where raw (case-sensitive) ordering of the sort column disagrees with
    # identity-key ordering: "Zeta" sorts before "alpha" by raw ASCII (Z <
    # a), but "zeta" sorts after "alpha" by identity key. Sorting pairs by
    # the bare case-sensitive spelling instead of the lowercased identity
    # would canonicalize the two declarations into different pair orders.
    declared = _foreign_key(("Zeta", "alpha"), ("z_id", "a_id"), name="t_fk")
    observed = _foreign_key(("ZETA", "ALPHA"), ("Z_ID", "A_ID"), name="T_FK")

    # Then they are recognized as the same constraint — canonicalization is
    # identity-keyed, not raw-string sorted
    assert declared == observed


# ---------- foreign key references


def test_foreign_key_reference_rejects_non_string_constraint_name() -> None:
    # When the referencing constraint's name is not a string, then construction fails
    with pytest.raises(TypeError):
        ForeignKeyReference(
            name=42,  # type: ignore[arg-type]
            referencing_table=_CUSTOMERS,
        )


def test_foreign_key_reference_rejects_blank_constraint_name() -> None:
    # When the referencing constraint's name is blank, then construction fails
    with pytest.raises(ValueError):
        ForeignKeyReference(name="  ", referencing_table=_CUSTOMERS)
