"""
Direct tests for the shared information_schema row -> domain mappers.

No Spark session, no fakes: mappers take plain attribute-style rows —
matching how the real query results are accessed — and return domain values.
"""

from types import SimpleNamespace

import pytest

from delta_engine.adapters.databricks.sql.rows import (
    column_tags_from_rows,
    referencing_foreign_keys_from_rows,
    table_tags_from_rows,
)
from delta_engine.domain.model import ForeignKeyReference, QualifiedName

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
