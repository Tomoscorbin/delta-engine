import json
from pathlib import Path

from hypothesis import given, strategies as st
import pytest

from delta_engine.adapters.databricks.sql.describe import (
    MetadataParseError,
    table_description_from_rows,
)
from delta_engine.domain.model import Integer, ObservedColumn, QualifiedName, String
from tests.adapters.databricks.sql.strategies import (
    CANONICAL_IDENTIFIERS,
    OBSERVED_TABLE_PROPERTIES,
    SQL_LITERAL_VALUES,
    TYPE_DOCUMENTS,
)

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


@st.composite
def _valid_describe_documents(draw: st.DrawFn):
    names = draw(st.lists(CANONICAL_IDENTIFIERS, min_size=1, max_size=5, unique=True))
    columns = []
    expected_columns = []
    raw_names = []
    for name in names:
        raw_name = draw(st.sampled_from((name, name.upper())))
        data_type, type_document = draw(TYPE_DOCUMENTS)
        nullable = draw(st.booleans())
        comment_kind = draw(st.sampled_from(("missing", "null", "value")))
        comment = draw(SQL_LITERAL_VALUES) if comment_kind == "value" else ""
        raw_names.append(raw_name)
        column_document = {
            "name": raw_name,
            "type": type_document,
            "nullable": nullable,
        }
        if comment_kind == "null":
            column_document["comment"] = None
        elif comment_kind == "value":
            column_document["comment"] = comment
        columns.append(column_document)
        expected_columns.append(
            ObservedColumn(
                name=raw_name.casefold(),
                data_type=data_type,
                nullable=nullable,
                comment=comment,
            )
        )

    partitioned_by = draw(st.lists(st.sampled_from(raw_names), unique=True))
    clustered_by = draw(st.lists(st.sampled_from(raw_names), unique=True))
    properties = draw(OBSERVED_TABLE_PROPERTIES)
    comment_kind = draw(st.sampled_from(("missing", "null", "value")))
    comment = draw(SQL_LITERAL_VALUES) if comment_kind == "value" else ""
    document = {
        "type": "MANAGED",
        "provider": "delta",
        "columns": columns,
        "partition_columns": partitioned_by,
        "clustering_columns": clustered_by,
        "table_properties": properties,
    }
    if comment_kind == "null":
        document["comment"] = None
    elif comment_kind == "value":
        document["comment"] = comment
    return (
        document,
        tuple(expected_columns),
        comment,
        tuple(name.casefold() for name in partitioned_by),
        tuple(name.casefold() for name in clustered_by),
        properties,
    )


# ---------- relation facts ----------


def test_relation_type_and_provider_are_carried():
    # Whether the engine reads a relation of this kind is the reader's
    # decision; the parse carries the facts verbatim.
    description = _parse(_doc(type="VIEW", provider="iceberg"))
    assert description.relation_type == "VIEW"
    assert description.provider == "iceberg"


def test_missing_or_non_string_relation_fields_carry_as_none():
    doc = json.loads(_doc())
    doc.pop("type")
    doc["provider"] = 7
    description = _parse(json.dumps(doc))
    assert description.relation_type is None
    assert description.provider is None


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


def test_table_properties_are_carried_verbatim():
    # Which property keys the engine manages is the reader's decision; the
    # parse carries every observed key, protocol internals included.
    properties = {
        "delta.columnMapping.mode": "name",
        "delta.feature.clustering": "supported",
        "delta.minReaderVersion": "3",
    }
    description = _parse(_doc(table_properties=properties))
    assert dict(description.table_properties) == properties


def test_absent_table_properties_carry_as_empty():
    assert dict(_parse(_doc()).table_properties) == {}


def test_non_object_table_properties_raises():
    with pytest.raises(MetadataParseError):
        _parse(_doc(table_properties="delta.appendOnly=true"))


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
    assert description.table_properties["delta.columnMapping.mode"] == "name"
    assert description.table_properties["delta.minReaderVersion"] == "3"  # unregistered, carried


@given(_valid_describe_documents())
def test_valid_describe_documents_preserve_values_and_normalize_identifiers(case) -> None:
    document, columns, comment, partitioned_by, clustered_by, properties = case

    description = _parse(json.dumps(document))

    assert description.columns == columns
    assert description.comment == comment
    assert description.partitioned_by == partitioned_by
    assert description.clustered_by == clustered_by
    assert dict(description.table_properties) == properties


@given(_valid_describe_documents())
def test_describe_parsing_ignores_json_formatting_key_order_and_unknown_fields(case) -> None:
    document = case[0]
    baseline = _parse(json.dumps(document, separators=(",", ":")))
    with_future_metadata = {**document, "future_metadata": {"ignored": [1, 2, 3]}}

    reparsed = _parse(json.dumps(with_future_metadata, indent=2, sort_keys=True))

    assert reparsed == baseline


@pytest.mark.parametrize(
    ("field", "malformed"),
    (
        ("table_comment", False),
        ("column_comment", 0),
        ("layout_item", {"name": "region"}),
        ("property_value", True),
    ),
)
def test_non_string_describe_leaf_values_raise_metadata_parse_error(
    field: str,
    malformed: object,
) -> None:
    document = json.loads(_doc())
    if field == "table_comment":
        document["comment"] = malformed
    elif field == "column_comment":
        document["columns"][0]["comment"] = malformed
    elif field == "layout_item":
        document["partition_columns"] = [malformed]
    else:
        document["table_properties"] = {"delta.appendOnly": malformed}

    with pytest.raises(MetadataParseError):
        _parse(json.dumps(document))
