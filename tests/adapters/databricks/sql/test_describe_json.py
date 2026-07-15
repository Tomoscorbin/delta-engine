"""Tests for structured DESCRIBE TABLE AS JSON metadata parsing."""

import json

import pytest

from delta_engine.adapters.databricks.sql.describe_json import (
    MetadataParseError,
    data_type_from_json,
    parse_described_table,
)
from delta_engine.domain.model import (
    Array,
    Decimal,
    Integer,
    Map,
    QualifiedName,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
)

QN = QualifiedName("dev", "silver", "demo_table")


def document(**overrides: object) -> str:
    value = {
        "table_name": "demo_table",
        "catalog_name": "dev",
        "schema_name": "silver",
        "type": "MANAGED",
        "provider": "delta",
        "columns": [
            {"name": "id", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
            {"name": "name", "type": {"name": "string"}, "nullable": True},
        ],
        "comment": "demo",
    }
    value.update(overrides)
    return json.dumps(value)


@pytest.mark.parametrize(
    ("type_object", "expected"),
    [
        ({"name": "integer"}, Integer()),
        ({"name": "varchar", "length": 20}, String()),
        ({"name": "timestamp"}, Timestamp()),
        ({"name": "timestamp_ltz"}, Timestamp()),
        ({"name": "timestamp_ntz"}, TimestampNtz()),
        ({"name": "decimal", "precision": 10, "scale": 2}, Decimal(10, 2)),
        ({"name": "array", "element_type": {"name": "string"}}, Array(String())),
        (
            {"name": "map", "key_type": {"name": "string"}, "value_type": {"name": "int"}},
            Map(String(), Integer()),
        ),
    ],
)
def test_maps_structured_type_objects(type_object, expected):
    assert data_type_from_json(type_object) == expected


def test_maps_nested_struct_and_preserves_direct_field_names():
    assert data_type_from_json(
        {
            "name": "struct",
            "fields": [
                {"name": "Bad Name", "type": {"name": "int"}},
                {"name": "tags", "type": {"name": "array", "element_type": {"name": "string"}}},
            ],
        }
    ) == Struct(
        (
            StructField("bad name", Integer()),
            StructField("tags", Array(String())),
        )
    )


@pytest.mark.parametrize(
    "type_object",
    [
        {"name": "geography"},
        {"name": "decimal", "precision": 40, "scale": 2},
        {"name": "decimal", "precision": "10", "scale": 2},
        {"name": "struct", "fields": []},
        {
            "name": "struct",
            "fields": [
                {"name": "a", "type": {"name": "int"}},
                {"name": "A", "type": {"name": "int"}},
            ],
        },
        {"not": "a type"},
        "string",
    ],
)
def test_unmappable_or_malformed_types_return_none(type_object):
    assert data_type_from_json(type_object) is None


def test_pathologically_deep_type_returns_none():
    payload: object = {"name": "int"}
    for _ in range(6000):
        payload = {"name": "array", "element_type": payload}
    assert data_type_from_json(payload) is None


def test_parses_columns_comment_and_partition_order():
    parsed = parse_described_table(
        document(
            columns=[
                {"name": "ID", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
                {"name": "Region", "type": {"name": "string"}, "nullable": True},
            ],
            comment=None,
            partition_columns=["Region"],
        ),
        QN,
    )

    assert [column.name for column in parsed.columns] == ["id", "region"]
    assert parsed.columns[0].nullable is False
    assert parsed.columns[0].comment == "pk"
    assert parsed.comment == ""
    assert parsed.partitioned_by == ("region",)


@pytest.mark.parametrize("relation_type", ["VIEW", "MATERIALIZED_VIEW", "STREAMING_TABLE"])
def test_unsupported_relation_types_fail_closed(relation_type):
    with pytest.raises(MetadataParseError, match="relation type"):
        parse_described_table(document(type=relation_type), QN)


def test_non_delta_provider_fails_closed():
    with pytest.raises(MetadataParseError, match="not Delta"):
        parse_described_table(document(provider="parquet"), QN)


@pytest.mark.parametrize("relation_type", ["MANAGED_SHALLOW_CLONE", "EXTERNAL_SHALLOW_CLONE"])
def test_delta_shallow_clone_table_types_are_supported(relation_type):
    parsed = parse_described_table(document(type=relation_type), QN)
    assert [column.name for column in parsed.columns] == ["id", "name"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"type": None},
        {"provider": None},
        {"columns": None},
        {"comment": 7},
        {"partition_columns": "region"},
        {"partition_columns": [7]},
        {"partition_columns": ["region", "REGION"]},
        {"columns": [{"name": "id", "type": {"name": "int"}, "nullable": "YES"}]},
        {"columns": [{"name": "id", "type": {"name": "int"}, "nullable": True, "comment": 7}]},
    ],
)
def test_malformed_metadata_fails_instead_of_defaulting(overrides):
    with pytest.raises(MetadataParseError):
        parse_described_table(document(**overrides), QN)


def test_partition_name_missing_from_schema_fails():
    with pytest.raises(MetadataParseError, match="not present"):
        parse_described_table(document(partition_columns=["region"]), QN)


def test_unmappable_partition_type_fails():
    with pytest.raises(MetadataParseError, match="cannot be read safely"):
        parse_described_table(
            document(
                partition_columns=["p"],
                columns=[{"name": "p", "type": {"name": "geography"}, "nullable": True}],
            ),
            QN,
        )


def test_unmappable_non_partition_type_is_skipped_with_warning(caplog):
    parsed = parse_described_table(
        document(
            columns=[
                {"name": "id", "type": {"name": "int"}, "nullable": True},
                {"name": "geo", "type": {"name": "geography"}, "nullable": True},
            ]
        ),
        QN,
    )

    assert [column.name for column in parsed.columns] == ["id"]
    assert "geo" in caplog.text


def test_invalid_json_and_non_object_document_fail():
    with pytest.raises(MetadataParseError, match="invalid JSON"):
        parse_described_table("{not json", QN)
    with pytest.raises(MetadataParseError, match="JSON object"):
        parse_described_table("[]", QN)
