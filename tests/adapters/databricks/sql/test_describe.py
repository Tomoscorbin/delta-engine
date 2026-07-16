import json
from pathlib import Path

import pytest

from delta_engine.adapters.databricks.sql.describe import (
    MetadataParseError,
    parse_table_description,
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
    description = parse_table_description(_doc(), QN)
    assert [c.name for c in description.columns] == ["id", "name"]
    assert description.columns[0].data_type == Integer()
    assert description.columns[0].nullable is False
    assert description.columns[0].comment == "pk"
    assert description.columns[1].data_type == String()
    assert description.columns[1].comment == ""  # omitted -> empty


def test_empty_table_comment_is_empty_string():
    assert parse_table_description(_doc(comment=""), QN).comment == ""
    doc = json.loads(_doc())
    doc.pop("comment")
    assert parse_table_description(json.dumps(doc), QN).comment == ""


def test_partitioning_and_clustering_casefolded_in_order():
    description = parse_table_description(
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
    assert description.partitioned_by == ("region", "store")
    assert description.clustered_by == ("id",)


def test_properties_filtered_to_registry():
    description = parse_table_description(
        _doc(
            table_properties={
                "delta.columnMapping.mode": "name",
                "delta.feature.clustering": "supported",
                "delta.minReaderVersion": "3",
            }
        ),
        QN,
    )
    assert dict(description.properties) == {"delta.columnMapping.mode": "name"}


def test_constraints_lowered_to_domain():
    description = parse_table_description(
        _doc(table_constraints="[(pk_demo,PRIMARY KEY (`id`))]"), QN
    )
    assert description.primary_key is not None
    assert description.primary_key.columns == ("id",)
    assert description.primary_key.constraint_name == "pk_demo"


def test_unmappable_non_partition_column_is_skipped():
    description = parse_table_description(
        _doc(
            columns=[
                {"name": "ok", "type": {"name": "int"}, "nullable": True},
                {"name": "weird", "type": {"name": "geography"}, "nullable": True},
            ]
        ),
        QN,
    )
    assert [c.name for c in description.columns] == ["ok"]


def test_malformed_json_and_missing_columns_raise():
    with pytest.raises(MetadataParseError):
        parse_table_description("{not json", QN)
    with pytest.raises(MetadataParseError):
        parse_table_description('{"comment": ""}', QN)


def test_malformed_table_constraints_raises_metadata_error():
    with pytest.raises(MetadataParseError):
        parse_table_description(_doc(table_constraints="[(pk_x,PRIMARY KEY)]"), QN)


def test_non_object_document_raises():
    with pytest.raises(MetadataParseError):
        parse_table_description("[1, 2, 3]", QN)


def test_malformed_column_entry_raises():
    with pytest.raises(MetadataParseError):
        parse_table_description(_doc(columns=[{"no_name": "x", "type": {"name": "int"}}]), QN)


def test_skipping_unmappable_column_logs_a_warning(caplog):
    import logging

    with caplog.at_level(logging.WARNING):
        description = parse_table_description(
            _doc(
                columns=[
                    {"name": "ok", "type": {"name": "int"}, "nullable": True},
                    {"name": "weird", "type": {"name": "geography"}, "nullable": True},
                ]
            ),
            QN,
        )
    assert [c.name for c in description.columns] == ["ok"]
    assert any("weird" in record.message for record in caplog.records)


_FIXTURES = Path(__file__).parent / "fixtures"


def test_real_order_fact_fixture():
    text = (_FIXTURES / "order_fact.json").read_text()
    description = parse_table_description(text, QualifiedName("dev", "gold", "order_fact"))
    assert len(description.columns) == 7
    assert description.columns[0].name == "order_id"
    assert description.columns[0].nullable is False
    assert description.primary_key.columns == ("order_id",)
    [fk] = description.foreign_keys
    assert fk.referenced_table == QualifiedName("dev", "gold", "product_dimension")
    assert dict(description.properties) == {"delta.columnMapping.mode": "name"}
