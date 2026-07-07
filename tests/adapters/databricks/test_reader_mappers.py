"""
Direct tests for the reader's pure row->domain mappers.

No Spark session, no fakes: mappers take plain rows (dicts for primary-key and
DESCRIBE DETAIL rows, attribute-style objects for FK/tag rows — matching how
the real query results are accessed) and return domain values.
"""

from types import SimpleNamespace

from pyspark.sql import Row
import pytest

from delta_engine.adapters.databricks.reader import (
    _column_tags_from_rows,
    _foreign_keys_from_rows,
    _managed_properties_from_row,
    _primary_key_from_rows,
    _referencing_foreign_keys_from_rows,
    _table_tags_from_rows,
)
from delta_engine.domain.model import ForeignKeyReference, QualifiedName
from delta_engine.domain.model.constraints import ForeignKeyConstraint, PrimaryKeyConstraint


def fk_row(
    *,
    constraint_name="fk_orders_customers",
    local_column="customer_id",
    ordinal_position=1,
    position_in_unique_constraint=1,
    ref_catalog="cat",
    ref_schema="sch",
    ref_table="customers",
    ref_column="id",
):
    return SimpleNamespace(
        constraint_name=constraint_name,
        local_column=local_column,
        ordinal_position=ordinal_position,
        position_in_unique_constraint=position_in_unique_constraint,
        ref_catalog=ref_catalog,
        ref_schema=ref_schema,
        ref_table=ref_table,
        ref_column=ref_column,
    )


# ---------- primary key ----------


def test_primary_key_mapper_returns_none_for_no_rows():
    assert _primary_key_from_rows([]) is None


def test_primary_key_mapper_lowercases_constraint_and_column_names():
    rows = [
        {"constraint_name": "PK_T", "column_name": "TENANT_ID"},
        {"constraint_name": "PK_T", "column_name": "Id"},
    ]
    assert _primary_key_from_rows(rows) == PrimaryKeyConstraint(
        columns=("tenant_id", "id"), constraint_name="pk_t"
    )


# ---------- foreign keys ----------


def test_foreign_keys_mapper_returns_empty_for_no_rows():
    assert _foreign_keys_from_rows([]) == ()


def test_foreign_keys_mapper_builds_single_column_fk_and_lowercases_names():
    rows = [
        fk_row(
            constraint_name="FK_Orders_Customers",
            local_column="Customer_ID",
            ref_catalog="Cat",
            ref_schema="Sch",
            ref_table="Customers",
            ref_column="ID",
        )
    ]
    assert _foreign_keys_from_rows(rows) == (
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=QualifiedName("cat", "sch", "customers"),
            referenced_columns=("id",),
            constraint_name="fk_orders_customers",
        ),
    )


def test_foreign_keys_mapper_aligns_composite_columns_positionally():
    # (tenant_id, customer_id) -> customers(tenant_id, id): one row per local
    # column, each carrying the parent-key column at the matching position.
    rows = [
        fk_row(local_column="tenant_id", ordinal_position=1, ref_column="tenant_id"),
        fk_row(local_column="customer_id", ordinal_position=2, ref_column="id"),
    ]
    (fk,) = _foreign_keys_from_rows(rows)
    assert fk.local_columns == ("tenant_id", "customer_id")
    assert fk.referenced_columns == ("tenant_id", "id")


def test_foreign_keys_mapper_groups_contiguous_rows_per_constraint():
    # Rows arrive ordered by (constraint_name, ordinal_position); each
    # contiguous run is one constraint.
    rows = [
        fk_row(constraint_name="fk_a", local_column="a_id", ref_table="a", ref_column="id"),
        fk_row(constraint_name="fk_b", local_column="b_id", ref_table="b", ref_column="id"),
    ]
    first, second = _foreign_keys_from_rows(rows)
    assert first.constraint_name == "fk_a"
    assert first.local_columns == ("a_id",)
    assert second.constraint_name == "fk_b"
    assert second.local_columns == ("b_id",)


# ---------- referencing foreign keys ----------


def test_referencing_foreign_keys_rows_map_to_casefolded_references() -> None:
    rows = [
        Row(
            constraint_name="Orders_Customer_FK",
            referencing_catalog="Dev",
            referencing_schema="Silver",
            referencing_table="Orders",
        ),
    ]

    result = _referencing_foreign_keys_from_rows(rows)

    assert result == (
        ForeignKeyReference(
            constraint_name="orders_customer_fk",
            referencing_table=QualifiedName("dev", "silver", "orders"),
        ),
    )


def test_referencing_foreign_keys_empty_rows_map_to_empty_tuple() -> None:
    assert _referencing_foreign_keys_from_rows([]) == ()


# ---------- table tags ----------


def test_table_tags_mapper_returns_empty_read_only_mapping_for_no_rows():
    tags = _table_tags_from_rows([])
    assert dict(tags) == {}
    with pytest.raises(TypeError):
        tags["x"] = "y"  # type: ignore[index]


def test_table_tags_mapper_preserves_tag_key_and_value_case():
    rows = [
        SimpleNamespace(tag_name="Owner", tag_value="Data-Platform"),
        SimpleNamespace(tag_name="tier", tag_value="Gold"),
    ]
    assert dict(_table_tags_from_rows(rows)) == {"Owner": "Data-Platform", "tier": "Gold"}


# ---------- column tags ----------


def test_column_tags_mapper_returns_empty_mapping_for_no_rows():
    assert dict(_column_tags_from_rows([])) == {}


def test_column_tags_mapper_lowercases_column_names_but_preserves_tag_case():
    rows = [
        SimpleNamespace(column_name="EMAIL", tag_name="PII", tag_value="Email"),
        SimpleNamespace(column_name="email", tag_name="mask", tag_value="hash"),
        SimpleNamespace(column_name="id", tag_name="key", tag_value="primary"),
    ]
    tags = _column_tags_from_rows(rows)
    assert dict(tags["email"]) == {"PII": "Email", "mask": "hash"}
    assert dict(tags["id"]) == {"key": "primary"}


# ---------- properties ----------


def test_properties_mapper_filters_to_registered_keys():
    row = {
        "properties": {
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "custom.unlisted": "dropped",
        }
    }
    assert dict(_managed_properties_from_row(row)) == {"delta.columnMapping.mode": "name"}


def test_properties_mapper_returns_empty_read_only_mapping_for_empty_map():
    properties = _managed_properties_from_row({"properties": {}})
    assert dict(properties) == {}
    with pytest.raises(TypeError):
        properties["x"] = "y"  # type: ignore[index]
