"""
Direct tests for the shared information_schema row -> domain mappers.

No Spark session, no fakes: mappers take plain attribute-style rows —
matching how the real query results are accessed — and return domain values.
"""

from types import SimpleNamespace

import pytest

from delta_engine.adapters.databricks.sql.rows import (
    column_tags_from_rows,
    foreign_keys_from_rows,
    primary_key_from_rows,
    referencing_foreign_keys_from_rows,
    table_tags_from_rows,
)
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
    QualifiedName,
)

# ---------- primary key ----------


def test_primary_key_rows_map_to_ordered_casefolded_columns() -> None:
    rows = [
        SimpleNamespace(constraint_name="Orders_PK", column_name="Order_Id"),
        SimpleNamespace(constraint_name="Orders_PK", column_name="Line_No"),
    ]

    result = primary_key_from_rows(rows)

    assert result == PrimaryKeyConstraint(
        columns=("order_id", "line_no"), constraint_name="orders_pk"
    )


def test_primary_key_empty_rows_map_to_none() -> None:
    assert primary_key_from_rows([]) is None


# ---------- owned foreign keys ----------


def test_foreign_key_rows_map_to_casefolded_constraint() -> None:
    rows = [
        SimpleNamespace(
            constraint_name="Orders_Customer_FK",
            local_column="Customer_Id",
            referenced_catalog="Dev",
            referenced_schema="Silver",
            referenced_table="Customer",
            referenced_column="Id",
        ),
    ]

    result = foreign_keys_from_rows(rows)

    assert result == (
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=QualifiedName("dev", "silver", "customer"),
            referenced_columns=("id",),
            constraint_name="orders_customer_fk",
        ),
    )


def test_composite_foreign_key_keeps_each_local_referenced_pair_together() -> None:
    # Rows arrive in the foreign key's column order; the mapper preserves each
    # (local, referenced) pair so the domain's canonical sort keeps them aligned.
    rows = [
        SimpleNamespace(
            constraint_name="fk_ab",
            local_column="b",
            referenced_catalog="c",
            referenced_schema="s",
            referenced_table="parent",
            referenced_column="y",
        ),
        SimpleNamespace(
            constraint_name="fk_ab",
            local_column="a",
            referenced_catalog="c",
            referenced_schema="s",
            referenced_table="parent",
            referenced_column="x",
        ),
    ]

    [fk] = foreign_keys_from_rows(rows)

    assert fk.local_columns == ("a", "b")
    assert fk.referenced_columns == ("x", "y")  # a->x and b->y preserved through the sort
    assert fk.referenced_table == QualifiedName("c", "s", "parent")


def test_multiple_foreign_keys_group_by_constraint_name() -> None:
    rows = [
        SimpleNamespace(
            constraint_name="fk_one",
            local_column="a",
            referenced_catalog="c",
            referenced_schema="s",
            referenced_table="p1",
            referenced_column="x",
        ),
        SimpleNamespace(
            constraint_name="fk_two",
            local_column="b",
            referenced_catalog="c",
            referenced_schema="s",
            referenced_table="p2",
            referenced_column="y",
        ),
    ]

    result = foreign_keys_from_rows(rows)

    assert len(result) == 2
    assert {fk.constraint_name for fk in result} == {"fk_one", "fk_two"}


def test_foreign_keys_empty_rows_map_to_empty_tuple() -> None:
    assert foreign_keys_from_rows([]) == ()


# ---------- referencing foreign keys ----------


def test_referencing_foreign_keys_rows_map_to_casefolded_references() -> None:
    rows = [
        SimpleNamespace(
            constraint_name="Orders_Customer_FK",
            referencing_catalog="Dev",
            referencing_schema="Silver",
            referencing_table="Orders",
        ),
    ]

    result = referencing_foreign_keys_from_rows(rows)

    assert result == (
        ForeignKeyReference(
            constraint_name="orders_customer_fk",
            referencing_table=QualifiedName("dev", "silver", "orders"),
        ),
    )


def test_referencing_foreign_keys_empty_rows_map_to_empty_tuple() -> None:
    assert referencing_foreign_keys_from_rows([]) == ()


# ---------- table tags ----------


def test_table_tags_mapper_returns_empty_read_only_mapping_for_no_rows():
    tags = table_tags_from_rows([])
    assert dict(tags) == {}
    with pytest.raises(TypeError):
        tags["x"] = "y"  # type: ignore[index]


def test_table_tags_mapper_preserves_tag_key_and_value_case():
    rows = [
        SimpleNamespace(tag_name="Owner", tag_value="Data-Platform"),
        SimpleNamespace(tag_name="tier", tag_value="Gold"),
    ]
    assert dict(table_tags_from_rows(rows)) == {"Owner": "Data-Platform", "tier": "Gold"}


# ---------- column tags ----------


def test_column_tags_mapper_returns_empty_mapping_for_no_rows():
    assert dict(column_tags_from_rows([])) == {}


def test_column_tags_mapper_lowercases_column_names_but_preserves_tag_case():
    rows = [
        SimpleNamespace(column_name="EMAIL", tag_name="PII", tag_value="Email"),
        SimpleNamespace(column_name="email", tag_name="mask", tag_value="hash"),
        SimpleNamespace(column_name="id", tag_name="key", tag_value="primary"),
    ]
    tags = column_tags_from_rows(rows)
    assert dict(tags["email"]) == {"PII": "Email", "mask": "hash"}
    assert dict(tags["id"]) == {"key": "primary"}
