"""
Direct tests for the shared catalog row -> domain mappers.

No Spark session, no fakes: mappers take plain attribute-style rows —
matching how the real query results are accessed — and return domain values.
DESCRIBE DETAIL rows use pyspark ``Row`` so the duck-typed contract
(attribute access plus ``asDict()``) is pinned against a real Row type.
"""

from types import SimpleNamespace

from pyspark.sql import Row
import pytest

from delta_engine.adapters.databricks.sql.rows import (
    UnsupportedCatalogRelationError,
    clustering_columns_from_detail_row,
    column_tags_from_rows,
    foreign_keys_from_rows,
    managed_properties_from_detail_row,
    primary_key_from_rows,
    referencing_foreign_keys_from_rows,
    require_delta_format,
    require_supported_relation,
    table_tags_from_rows,
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
    assert primary_key_from_rows([]) is None


def test_primary_key_mapper_lowercases_constraint_and_column_names():
    rows = [
        Row(constraint_name="PK_T", column_name="TENANT_ID"),
        Row(constraint_name="PK_T", column_name="Id"),
    ]
    assert primary_key_from_rows(rows) == PrimaryKeyConstraint(
        columns=("tenant_id", "id"), constraint_name="pk_t"
    )


# ---------- foreign keys ----------


def test_foreign_keys_mapper_returns_empty_for_no_rows():
    assert foreign_keys_from_rows([]) == ()


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
    assert foreign_keys_from_rows(rows) == (
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
    (fk,) = foreign_keys_from_rows(rows)

    # The constraint stores pairs canonically (sorted by local column), so
    # customer_id sorts before tenant_id even though rows arrived tenant-first.
    assert fk.local_columns == ("customer_id", "tenant_id")
    assert fk.referenced_columns == ("id", "tenant_id")


def test_foreign_keys_mapper_groups_contiguous_rows_per_constraint():
    # Rows arrive ordered by (constraint_name, ordinal_position); each
    # contiguous run is one constraint.
    rows = [
        fk_row(constraint_name="fk_a", local_column="a_id", ref_table="a", ref_column="id"),
        fk_row(constraint_name="fk_b", local_column="b_id", ref_table="b", ref_column="id"),
    ]
    first, second = foreign_keys_from_rows(rows)
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


# ---------- DESCRIBE DETAIL: properties + clustering ----------


def test_detail_properties_filter_to_registered_keys():
    row = Row(
        properties={
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "custom.unlisted": "dropped",
        }
    )
    assert dict(managed_properties_from_detail_row(row)) == {"delta.columnMapping.mode": "name"}


def test_detail_properties_accepts_json_string_and_native_mapping():
    native = Row(properties={"delta.enableChangeDataFeed": "true"})
    encoded = Row(properties='{"delta.enableChangeDataFeed": "true"}')

    expected = {"delta.enableChangeDataFeed": "true"}
    assert dict(managed_properties_from_detail_row(native)) == expected
    assert dict(managed_properties_from_detail_row(encoded)) == expected


def test_detail_properties_null_or_empty_means_no_properties():
    properties = managed_properties_from_detail_row(Row(properties=None))
    assert dict(properties) == {}
    assert dict(managed_properties_from_detail_row(Row(properties="{}"))) == {}
    with pytest.raises(TypeError):
        properties["x"] = "y"  # type: ignore[index]


def test_detail_clustering_accepts_json_string_and_native_array_and_casefolds():
    native = Row(clusteringColumns=["Region", "STORE"])
    encoded = Row(clusteringColumns='["Region", "STORE"]')

    assert clustering_columns_from_detail_row(native) == ("region", "store")
    assert clustering_columns_from_detail_row(encoded) == ("region", "store")


def test_detail_clustering_absent_field_or_empty_array_means_unclustered():
    assert clustering_columns_from_detail_row(Row(properties={})) == ()
    assert clustering_columns_from_detail_row(Row(clusteringColumns=[])) == ()
    assert clustering_columns_from_detail_row(Row(clusteringColumns="[]")) == ()


QN = QualifiedName("cat", "sch", "tbl")


# ---------- relation-kind guard ----------


@pytest.mark.parametrize("table_type", ["MANAGED", "EXTERNAL", "managed", "external"])
def test_require_supported_relation_admits_ordinary_delta_tables(table_type):
    require_supported_relation(table_type, QN)  # does not raise


@pytest.mark.parametrize(
    "table_type",
    [
        "VIEW",
        "MATERIALIZED_VIEW",
        "STREAMING_TABLE",
        "FOREIGN",
        "MANAGED_SHALLOW_CLONE",
        "EXTERNAL_SHALLOW_CLONE",
        "SOME_FUTURE_KIND",
    ],
)
def test_require_supported_relation_rejects_every_other_kind(table_type):
    with pytest.raises(UnsupportedCatalogRelationError):
        require_supported_relation(table_type, QN)


def test_require_supported_relation_names_the_object_and_kind():
    with pytest.raises(UnsupportedCatalogRelationError, match="STREAMING_TABLE"):
        require_supported_relation("STREAMING_TABLE", QN)


# ---------- format guard ----------


@pytest.mark.parametrize("table_format", ["delta", "DELTA", "Delta"])
def test_require_delta_format_admits_delta(table_format):
    require_delta_format(Row(format=table_format), QN)  # does not raise


@pytest.mark.parametrize("table_format", ["iceberg", "parquet", "csv"])
def test_require_delta_format_rejects_non_delta(table_format):
    with pytest.raises(UnsupportedCatalogRelationError):
        require_delta_format(Row(format=table_format), QN)
