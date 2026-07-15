import json
from pathlib import Path

import pytest

from delta_engine.adapters.databricks.sql.describe import (
    MetadataParseError,
    parse_table_snapshot,
)
from delta_engine.domain.model import Integer, QualifiedName, String

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
