# tests/adapters/databricks/sql/test_describe_json.py
import json
from pathlib import Path

import pytest

from delta_engine.adapters.databricks.sql.describe_json import (
    MetadataParseError,
    data_type_from_json,
    parse_table_snapshot,
)
from delta_engine.domain.model import (
    Array,
    Boolean,
    Decimal,
    Double,
    Integer,
    Long,
    Map,
    QualifiedName,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
)


def test_primitive_aliases():
    assert data_type_from_json({"name": "int"}) == Integer()
    assert data_type_from_json({"name": "integer"}) == Integer()
    assert data_type_from_json({"name": "bigint"}) == Long()
    assert data_type_from_json({"name": "double"}) == Double()
    assert data_type_from_json({"name": "boolean"}) == Boolean()


def test_string_ignores_collation_and_length():
    assert data_type_from_json({"name": "string", "collation": "UTF8_BINARY"}) == String()
    assert data_type_from_json({"name": "varchar", "length": 20}) == String()


def test_timestamp_ltz_aliases_to_timestamp():
    assert data_type_from_json({"name": "timestamp"}) == Timestamp()
    assert data_type_from_json({"name": "timestamp_ltz"}) == Timestamp()
    assert data_type_from_json({"name": "timestamp_ntz"}) == TimestampNtz()


def test_decimal_reads_precision_and_scale():
    assert data_type_from_json({"name": "decimal", "precision": 10, "scale": 2}) == Decimal(10, 2)


def test_array_map_struct_nested():
    assert data_type_from_json(
        {"name": "array", "element_type": {"name": "string"}, "element_nullable": True}
    ) == Array(String())
    assert data_type_from_json(
        {"name": "map", "key_type": {"name": "string"}, "value_type": {"name": "int"}}
    ) == Map(String(), Integer())
    assert data_type_from_json(
        {
            "name": "struct",
            "fields": [
                {"name": "Age", "type": {"name": "int"}, "nullable": True},
                {"name": "label", "type": {"name": "string"}, "nullable": True},
            ],
        }
    ) == Struct((StructField("age", Integer()), StructField("label", String())))


def test_unmappable_returns_none():
    assert data_type_from_json({"name": "interval"}) is None
    assert (
        data_type_from_json(
            {
                "name": "struct",
                "fields": [
                    {"name": "a", "type": {"name": "int"}},
                    {"name": "A", "type": {"name": "int"}},
                ],
            }
        )
        is None
    )  # duplicate field name after casefold
    assert data_type_from_json({"not": "a type"}) is None
    assert data_type_from_json("string") is None


def test_blank_struct_field_name_returns_none():
    assert (
        data_type_from_json({"name": "struct", "fields": [{"name": "  ", "type": {"name": "int"}}]})
        is None
    )


def test_decimal_over_delta_limit_returns_none():
    assert data_type_from_json({"name": "decimal", "precision": 40, "scale": 2}) is None


def test_pathologically_deep_nesting_returns_none():
    payload = {"name": "int"}
    for _ in range(6000):
        payload = {"name": "array", "element_type": payload}
    assert data_type_from_json(payload) is None


QN = QualifiedName("dev", "silver", "demo_table")


def _doc(**overrides):
    base = {
        "table_name": "demo_table",
        "catalog_name": "dev",
        "schema_name": "silver",
        "columns": [
            {"name": "id", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
            {
                "name": "name",
                "type": {"name": "string", "collation": "UTF8_BINARY"},
                "nullable": True,
            },
        ],
        "comment": "",
    }
    base.update(overrides)
    return json.dumps(base)


def test_columns_types_nullability_comments_and_order():
    snap = parse_table_snapshot(_doc(), QN)
    assert [c.name for c in snap.columns] == ["id", "name"]
    assert snap.columns[0].data_type == Integer()
    assert snap.columns[0].nullable is False
    assert snap.columns[0].comment == "pk"
    assert snap.columns[1].data_type == String()
    assert snap.columns[1].comment == ""  # omitted -> empty


def test_empty_table_comment_is_empty_string():
    assert parse_table_snapshot(_doc(comment=""), QN).comment == ""
    doc = json.loads(_doc())
    doc.pop("comment")
    assert parse_table_snapshot(json.dumps(doc), QN).comment == ""


def test_partitioning_and_clustering_casefolded_in_order():
    snap = parse_table_snapshot(
        _doc(
            partition_columns=["Region", "Store"],
            clustering_columns=["ID"],
            columns=[
                {"name": "id", "type": {"name": "int"}, "nullable": True},
                {"name": "region", "type": {"name": "string"}, "nullable": True},
                {"name": "store", "type": {"name": "string"}, "nullable": True},
            ],
        ),
        QN,
    )
    assert snap.partitioned_by == ("region", "store")
    assert snap.clustered_by == ("id",)


def test_properties_filtered_to_registry():
    snap = parse_table_snapshot(
        _doc(
            table_properties={
                "delta.columnMapping.mode": "name",
                "delta.feature.clustering": "supported",
                "delta.minReaderVersion": "3",
            }
        ),
        QN,
    )
    assert dict(snap.properties) == {"delta.columnMapping.mode": "name"}


def test_constraints_lowered_to_domain():
    snap = parse_table_snapshot(_doc(table_constraints="[(pk_demo,PRIMARY KEY (`id`))]"), QN)
    assert snap.primary_key is not None
    assert snap.primary_key.columns == ("id",)
    assert snap.primary_key.constraint_name == "pk_demo"


def test_unmappable_non_partition_column_is_skipped():
    snap = parse_table_snapshot(
        _doc(
            columns=[
                {"name": "ok", "type": {"name": "int"}, "nullable": True},
                {"name": "weird", "type": {"name": "geography"}, "nullable": True},
            ]
        ),
        QN,
    )
    assert [c.name for c in snap.columns] == ["ok"]


def test_unmappable_partition_column_raises():
    with pytest.raises(MetadataParseError):
        parse_table_snapshot(
            _doc(
                partition_columns=["p"],
                columns=[{"name": "p", "type": {"name": "geography"}, "nullable": True}],
            ),
            QN,
        )


def test_malformed_json_and_missing_columns_raise():
    with pytest.raises(MetadataParseError):
        parse_table_snapshot("{not json", QN)
    with pytest.raises(MetadataParseError):
        parse_table_snapshot('{"comment": ""}', QN)


def test_malformed_table_constraints_raises_metadata_error():
    with pytest.raises(MetadataParseError):
        parse_table_snapshot(_doc(table_constraints="[(pk_x,PRIMARY KEY)]"), QN)


def test_non_object_document_raises():
    with pytest.raises(MetadataParseError):
        parse_table_snapshot("[1, 2, 3]", QN)


def test_malformed_column_entry_raises():
    with pytest.raises(MetadataParseError):
        parse_table_snapshot(_doc(columns=[{"no_name": "x", "type": {"name": "int"}}]), QN)


def test_skipping_unmappable_column_logs_a_warning(caplog):
    import logging

    with caplog.at_level(logging.WARNING):
        snap = parse_table_snapshot(
            _doc(
                columns=[
                    {"name": "ok", "type": {"name": "int"}, "nullable": True},
                    {"name": "weird", "type": {"name": "geography"}, "nullable": True},
                ]
            ),
            QN,
        )
    assert [c.name for c in snap.columns] == ["ok"]
    assert any("weird" in record.message for record in caplog.records)


_FIXTURES = Path(__file__).parent / "fixtures"


def test_real_order_fact_fixture():
    text = (_FIXTURES / "order_fact.json").read_text()
    snap = parse_table_snapshot(text, QualifiedName("dev", "gold", "order_fact"))
    assert len(snap.columns) == 7
    assert snap.columns[0].name == "order_id"
    assert snap.columns[0].nullable is False
    assert snap.primary_key.columns == ("order_id",)
    [fk] = snap.foreign_keys
    assert fk.referenced_table == QualifiedName("dev", "gold", "product_dimension")
    assert dict(snap.properties) == {"delta.columnMapping.mode": "name"}
