import pytest

from delta_engine.domain.model.foreign_key import ForeignKeyConstraint


def test_resolve_constraint_name_uses_explicit_name_when_provided():
    # Given a constraint with an explicit name
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="main.sales.customers",
        referenced_columns=("id",),
        constraint_name="my_explicit_fk",
    )

    # When resolving the constraint name
    name = fk.resolve_constraint_name("orders")

    # Then the explicit name is returned
    assert name == "my_explicit_fk"


def test_resolve_constraint_name_derives_from_table_and_columns_when_not_provided():
    # Given a constraint with no explicit name
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="main.sales.customers",
        referenced_columns=("id",),
    )

    # When resolving the constraint name
    name = fk.resolve_constraint_name("orders")

    # Then the name is derived deterministically
    assert name == "orders_customer_id_fk"


def test_resolve_constraint_name_joins_multiple_local_columns_with_underscore():
    # Given a composite FK with no explicit name
    fk = ForeignKeyConstraint(
        local_columns=("tenant_id", "customer_id"),
        references="main.sales.customers",
        referenced_columns=("tenant_id", "id"),
    )

    # When resolving
    name = fk.resolve_constraint_name("orders")

    # Then all local columns are joined
    assert name == "orders_tenant_id_customer_id_fk"


def test_rejects_empty_local_columns():
    # Given / When / Then
    with pytest.raises(ValueError, match="local_columns"):
        ForeignKeyConstraint(
            local_columns=(),
            references="main.sales.customers",
            referenced_columns=("id",),
        )


def test_rejects_empty_referenced_columns():
    with pytest.raises(ValueError, match="referenced_columns"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            references="main.sales.customers",
            referenced_columns=(),
        )


def test_rejects_mismatched_column_counts():
    with pytest.raises(ValueError, match="must have the same number"):
        ForeignKeyConstraint(
            local_columns=("a", "b"),
            references="main.sales.customers",
            referenced_columns=("id",),
        )


def test_rejects_references_without_three_parts():
    # Given / When / Then
    with pytest.raises(ValueError, match=r"catalog\.schema\.table"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            references="schema.table",  # missing catalog
            referenced_columns=("id",),
        )


def test_rejects_references_with_uppercase_characters():
    # Given a references value that is not lowercase
    # When / Then — must match QualifiedName's lowercase rule so registry lookups align
    with pytest.raises(ValueError, match="lowercase"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            references="MAIN.sales.customers",
            referenced_columns=("id",),
        )


def test_rejects_references_with_a_blank_part():
    # Given a references value with an empty middle part (".sch.tbl"-style)
    # When / Then — every part must be a real identifier so compiled SQL is valid
    with pytest.raises(ValueError, match="blank"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            references="main..customers",
            referenced_columns=("id",),
        )


def test_rejects_blank_explicit_constraint_name():
    # Given an explicit but blank constraint name
    # When / Then — a blank name would compile to invalid `ADD CONSTRAINT `` ...`
    with pytest.raises(ValueError, match="constraint_name"):
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            references="main.sales.customers",
            referenced_columns=("id",),
            constraint_name="   ",
        )


def test_foreign_key_constraint_is_frozen():
    # Given
    fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="main.sales.customers",
        referenced_columns=("id",),
    )

    # When / Then — frozen dataclass raises on mutation
    with pytest.raises(AttributeError):
        fk.references = "other"  # type: ignore[misc]
