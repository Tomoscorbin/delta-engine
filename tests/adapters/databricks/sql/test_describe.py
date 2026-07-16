import json
from pathlib import Path

import pytest

from delta_engine.adapters.databricks.sql.describe import (
    MetadataParseError,
    UnsupportedRelationError,
    table_description_from_rows,
)
from delta_engine.domain.model import Integer, QualifiedName, String

QN = QualifiedName("dev", "silver", "demo_table")


def _parse(json_text, qualified_name=QN):
    """Wrap the document the way the describe query returns it: one row, one column."""
    return table_description_from_rows([(json_text,)], qualified_name)


def _doc(**overrides):
    base = {
        "table_name": "demo_table",
        "catalog_name": "dev",
        "schema_name": "silver",
        "type": "MANAGED",
        "provider": "delta",
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


# ---------- relation acceptance ----------


def test_managed_delta_table_is_readable():
    assert _parse(_doc()).qualified_name == QN


def test_external_delta_table_is_readable():
    description = _parse(_doc(type="EXTERNAL"))
    assert [column.name for column in description.columns] == ["id", "name"]


def test_relations_that_are_not_tables_are_not_readable():
    # The engine manages ordinary tables. Every other relation kind fails the
    # read — including kinds Databricks adds in the future — rather than being
    # diffed and planned against as though it were one.
    for kind in ("VIEW", "MATERIALIZED_VIEW", "STREAMING_TABLE", "FOREIGN", "FUTURE_KIND"):
        with pytest.raises(UnsupportedRelationError):
            _parse(_doc(type=kind))


def test_non_delta_formats_are_not_readable():
    for provider in ("iceberg", "parquet", "csv"):
        with pytest.raises(UnsupportedRelationError):
            _parse(_doc(provider=provider))


def test_document_without_relation_kind_or_provider_fails_closed():
    for missing in ("type", "provider"):
        doc = json.loads(_doc())
        doc.pop(missing)
        with pytest.raises(UnsupportedRelationError):
            _parse(json.dumps(doc))


def test_rejection_message_names_the_found_relation_and_the_supported_kinds():
    with pytest.raises(UnsupportedRelationError) as excinfo:
        _parse(_doc(type="STREAMING_TABLE"))
    message = str(excinfo.value)
    assert "STREAMING_TABLE" in message
    assert "MANAGED or EXTERNAL" in message


# ---------- columns ----------


def test_columns_types_nullability_comments_and_order():
    description = _parse(_doc())
    assert [c.name for c in description.columns] == ["id", "name"]
    assert description.columns[0].data_type == Integer()
    assert description.columns[0].nullable is False
    assert description.columns[0].comment == "pk"
    assert description.columns[1].data_type == String()
    assert description.columns[1].comment == ""  # omitted -> empty


def test_empty_table_comment_is_empty_string():
    assert _parse(_doc(comment="")).comment == ""
    doc = json.loads(_doc())
    doc.pop("comment")
    assert _parse(json.dumps(doc)).comment == ""


def test_partitioning_and_clustering_casefolded_in_order():
    description = _parse(
        _doc(
            partition_columns=["Region", "Store"],
            clustering_columns=["ID"],
            columns=[
                {"name": "id", "type": {"name": "int"}, "nullable": True},
                {"name": "region", "type": {"name": "string"}, "nullable": True},
                {"name": "store", "type": {"name": "string"}, "nullable": True},
            ],
        )
    )
    assert description.partitioned_by == ("region", "store")
    assert description.clustered_by == ("id",)


def test_non_list_partition_columns_raises():
    # A present-but-non-list layout field is drift, not "no partitioning".
    with pytest.raises(MetadataParseError):
        _parse(_doc(partition_columns="region"))


def test_non_list_clustering_columns_raises():
    with pytest.raises(MetadataParseError):
        _parse(_doc(clustering_columns="id"))


def test_properties_filtered_to_registry():
    description = _parse(
        _doc(
            table_properties={
                "delta.columnMapping.mode": "name",
                "delta.feature.clustering": "supported",
                "delta.minReaderVersion": "3",
            }
        )
    )
    assert dict(description.properties) == {"delta.columnMapping.mode": "name"}


def test_unsupported_column_type_fails_the_read():
    # An unknown or future type name is a column the domain cannot model. The
    # engine owns the full column set, so dropping it would read as "in sync";
    # fail the read instead.
    with pytest.raises(MetadataParseError):
        _parse(
            _doc(
                columns=[
                    {"name": "ok", "type": {"name": "int"}, "nullable": True},
                    {"name": "weird", "type": {"name": "geography"}, "nullable": True},
                ]
            )
        )


def test_malformed_type_object_raises():
    # A non-object type is a malformed shape, caught before type classification.
    with pytest.raises(MetadataParseError):
        _parse(
            _doc(
                columns=[
                    {"name": "id", "type": {"name": "int"}, "nullable": False},
                    {"name": "amount", "type": "decimal"},
                ]
            )
        )


def test_type_object_without_name_raises():
    with pytest.raises(MetadataParseError):
        _parse(
            _doc(
                columns=[
                    {"name": "id", "type": {"name": "int"}, "nullable": False},
                    {"name": "amount", "type": {"precision": 10, "scale": 2}},
                ]
            )
        )


def test_unsupported_nested_type_fails_the_read():
    # A nested type the domain cannot represent (here an array with no element
    # type) is unreadable just like an unknown top-level type: fail, don't drop.
    with pytest.raises(MetadataParseError):
        _parse(
            _doc(
                columns=[
                    {"name": "id", "type": {"name": "int"}, "nullable": False},
                    {"name": "tags", "type": {"name": "array"}, "nullable": True},
                ]
            )
        )


def test_non_boolean_nullable_fails_the_read():
    with pytest.raises(MetadataParseError):
        _parse(_doc(columns=[{"name": "id", "type": {"name": "int"}, "nullable": "false"}]))


def test_empty_describe_result_raises():
    # The statement returns exactly one row; no rows means the table could not
    # be described, not that it is absent.
    with pytest.raises(MetadataParseError):
        table_description_from_rows([], QN)


def test_malformed_json_and_missing_columns_raise():
    with pytest.raises(MetadataParseError):
        _parse("{not json")
    doc = json.loads(_doc())
    doc.pop("columns")
    with pytest.raises(MetadataParseError):
        _parse(json.dumps(doc))


def test_non_object_document_raises():
    with pytest.raises(MetadataParseError):
        _parse("[1, 2, 3]")


def test_malformed_column_entry_raises():
    with pytest.raises(MetadataParseError):
        _parse(_doc(columns=[{"no_name": "x", "type": {"name": "int"}}]))


_FIXTURES = Path(__file__).parent / "fixtures"


def test_real_order_fact_fixture():
    text = (_FIXTURES / "order_fact.json").read_text()
    description = _parse(text, QualifiedName("dev", "gold", "order_fact"))
    assert len(description.columns) == 7
    assert description.columns[0].name == "order_id"
    assert description.columns[0].nullable is False
    assert dict(description.properties) == {"delta.columnMapping.mode": "name"}
