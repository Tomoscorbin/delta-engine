import json
from pathlib import Path
from typing import NamedTuple

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
    TYPE_CASES,
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


def _doc_without(key: str) -> str:
    document = json.loads(_doc())
    document.pop(key)
    return json.dumps(document)


class DescribeCase(NamedTuple):
    """A generated valid document paired with the observed state it must parse to."""

    document: dict
    columns: tuple[ObservedColumn, ...]
    comment: str
    partitioned_by: tuple[str, ...]
    clustered_by: tuple[str, ...]
    properties: dict[str, str]


@st.composite
def _valid_describe_documents(draw: st.DrawFn) -> DescribeCase:
    names = draw(st.lists(CANONICAL_IDENTIFIERS, min_size=1, max_size=5, unique=True))
    columns = []
    expected_columns = []
    raw_names = []
    for name in names:
        raw_name = draw(st.sampled_from((name, name.upper())))
        type_case = draw(TYPE_CASES)
        nullable = draw(st.booleans())
        comment_kind = draw(st.sampled_from(("missing", "null", "value")))
        comment = draw(SQL_LITERAL_VALUES) if comment_kind == "value" else ""
        raw_names.append(raw_name)
        column_document = {
            "name": raw_name,
            "type": type_case.document,
            "nullable": nullable,
        }
        if comment_kind == "null":
            column_document["comment"] = None
        elif comment_kind == "value":
            column_document["comment"] = comment
        columns.append(column_document)
        expected_columns.append(
            ObservedColumn(
                name=raw_name,
                data_type=type_case.data_type,
                nullable=nullable,
                comment=comment,
            )
        )

    # A Delta table is partitioned or liquid-clustered, never both.
    layout_kind = draw(st.sampled_from(("none", "partitioned", "clustered")))
    layout_columns = (
        draw(st.lists(st.sampled_from(raw_names), unique=True, min_size=1))
        if layout_kind != "none"
        else []
    )
    partitioned_by = layout_columns if layout_kind == "partitioned" else []
    clustered_by = layout_columns if layout_kind == "clustered" else []
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
    return DescribeCase(
        document=document,
        columns=tuple(expected_columns),
        comment=comment,
        # Layout lists are catalog spelling and stay verbatim.
        partitioned_by=tuple(partitioned_by),
        clustered_by=tuple(clustered_by),
        properties=properties,
    )


# ---------- relation facts ----------


def test_relation_type_and_provider_are_carried():
    # Given a relation the engine does not manage
    description = _parse(_doc(type="VIEW", provider="iceberg"))

    # Then the parse carries the facts verbatim — whether the engine reads a
    # relation of this kind is the reader's decision
    assert description.relation_type == "VIEW"
    assert description.provider == "iceberg"


def test_missing_or_non_string_relation_fields_carry_as_none():
    # Given a document missing its type and carrying a non-string provider
    doc = json.loads(_doc())
    doc.pop("type")
    doc["provider"] = 7

    description = _parse(json.dumps(doc))

    # Then both facts carry as unknown rather than failing the parse
    assert description.relation_type is None
    assert description.provider is None


# ---------- columns ----------


def test_columns_types_nullability_comments_and_order():
    description = _parse(_doc())

    # Then columns carry their declared order, types, nullability, and comments
    assert [c.name for c in description.columns] == ["id", "name"]
    assert description.columns[0].data_type == Integer()
    assert description.columns[0].nullable is False
    assert description.columns[0].comment == "pk"
    assert description.columns[1].data_type == String()
    assert description.columns[1].comment == ""  # omitted -> empty


def test_empty_table_comment_is_empty_string():
    # Then an empty and an omitted comment both read as the empty string
    assert _parse(_doc(comment="")).comment == ""
    doc = json.loads(_doc())
    doc.pop("comment")
    assert _parse(json.dumps(doc)).comment == ""


def test_mixed_case_column_name_is_preserved_verbatim():
    # Given DESCRIBE echoing the creator's casing ('MyCol')
    description = _parse(
        _doc(columns=[{"name": "MyCol", "type": {"name": "int"}, "nullable": True}])
    )

    # Then the spelling carries without lowercasing, so the observed column
    # displays its exact casing while its identity stays case-insensitive
    assert str(description.columns[0].name) == "MyCol"
    assert description.columns[0].name == "mycol"


def test_lowercase_unicode_column_name_is_preserved_verbatim():
    # Given 'straße', which is already lowercase; casefold would rewrite it to
    # 'strasse', a different identifier from the one the catalog stores
    description = _parse(
        _doc(columns=[{"name": "straße", "type": {"name": "int"}, "nullable": True}])
    )

    # Then the catalog's spelling survives
    assert str(description.columns[0].name) == "straße"


def test_partitioning_and_clustering_carried_verbatim_in_order():
    # Given layout lists carrying the catalog's mixed-case spelling
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

    # Then the spelling and order carry verbatim — identifier normalization
    # happens once, in the domain constructors, not in the adapter carrier
    assert description.partitioned_by == ("Region", "Store")
    assert description.clustered_by == ("ID",)


def test_table_properties_are_carried_verbatim():
    # Given observed properties including protocol internals
    properties = {
        "delta.columnMapping.mode": "name",
        "delta.feature.clustering": "supported",
        "delta.minReaderVersion": "3",
    }

    description = _parse(_doc(table_properties=properties))

    # Then every observed key carries — which keys the engine manages is the
    # reader's decision, not the parser's
    assert dict(description.table_properties) == properties


def test_absent_table_properties_carry_as_empty():
    # Then a document without properties reads as an empty mapping
    assert dict(_parse(_doc()).table_properties) == {}


def test_empty_describe_result_raises():
    # Given no row from a statement that must return exactly one

    # When parsing the result
    with pytest.raises(MetadataParseError) as exc_info:
        table_description_from_rows([], QN)

    # Then absence is diagnosed as an invalid description, not a missing table
    assert "returned no rows" in str(exc_info.value)


@pytest.mark.parametrize(
    ("json_text", "diagnostic"),
    [
        pytest.param(
            _doc(partition_columns="region"),
            "partition_columns is not a list",
            id="partition-columns-not-list",
        ),
        pytest.param(
            _doc(clustering_columns="id"),
            "clustering_columns is not a list",
            id="clustering-columns-not-list",
        ),
        pytest.param(
            _doc(table_properties="delta.appendOnly=true"),
            "table_properties is not an object",
            id="properties-not-object",
        ),
        pytest.param(
            _doc(
                columns=[
                    {"name": "ok", "type": {"name": "int"}, "nullable": True},
                    {"name": "weird", "type": {"name": "geography"}, "nullable": True},
                ]
            ),
            "column 'weird' has an unsupported type",
            id="unsupported-column-type",
        ),
        pytest.param(
            _doc(columns=[{"name": "amount", "type": "decimal"}]),
            "column 'amount' has a malformed type object",
            id="type-not-object",
        ),
        pytest.param(
            _doc(columns=[{"name": "amount", "type": {"precision": 10, "scale": 2}}]),
            "column 'amount' has a malformed type object",
            id="type-name-missing",
        ),
        pytest.param(
            _doc(columns=[{"name": "tags", "type": {"name": "array"}, "nullable": True}]),
            "column 'tags' has an unsupported type",
            id="unsupported-nested-type",
        ),
        pytest.param(
            _doc(columns=[{"name": "id", "type": {"name": "int"}, "nullable": "false"}]),
            "column 'id' has a non-boolean nullable",
            id="nullable-not-boolean",
        ),
        pytest.param("{not json", "was not valid JSON", id="invalid-json"),
        pytest.param(_doc_without("columns"), "has no columns array", id="columns-missing"),
        pytest.param("[1, 2, 3]", "expected a JSON object", id="document-not-object"),
        pytest.param(
            _doc(columns=[{"no_name": "x", "type": {"name": "int"}}]),
            "malformed column entry",
            id="column-name-missing",
        ),
    ],
)
def test_invalid_describe_documents_raise_with_diagnostic(
    json_text: str,
    diagnostic: str,
) -> None:
    # Given malformed catalog metadata

    # When parsing the document
    with pytest.raises(MetadataParseError) as exc_info:
        _parse(json_text)

    # Then the diagnostic identifies the unsupported shape
    assert diagnostic in str(exc_info.value)


_FIXTURES = Path(__file__).parent / "fixtures"


def test_real_order_fact_fixture():
    # Given a real DESCRIBE document captured from Unity Catalog
    text = (_FIXTURES / "order_fact.json").read_text()

    description = _parse(text, QualifiedName("dev", "gold", "order_fact"))

    # Then the parse reads the captured table faithfully
    assert len(description.columns) == 7
    assert description.columns[0].name == "order_id"
    assert description.columns[0].nullable is False
    assert description.table_properties["delta.columnMapping.mode"] == "name"
    assert description.table_properties["delta.minReaderVersion"] == "3"  # unregistered, carried


@given(_valid_describe_documents())
def test_valid_describe_documents_preserve_values_and_identifier_spelling(
    case: DescribeCase,
) -> None:
    # When parsing
    description = _parse(json.dumps(case.document))

    # Then every carried field survives verbatim, including identifier spelling
    assert description.columns == case.columns
    assert tuple(str(c.name) for c in description.columns) == tuple(
        str(c.name) for c in case.columns
    )
    assert description.comment == case.comment
    assert description.partitioned_by == case.partitioned_by
    assert description.clustered_by == case.clustered_by
    assert dict(description.table_properties) == case.properties


def test_unknown_describe_fields_are_ignored() -> None:
    # Given a document carrying a field the parser does not know
    baseline = _parse(_doc())

    # Then the read is unaffected — Unity Catalog grows the document over time
    assert _parse(_doc(future_metadata={"ignored": [1, 2, 3]})) == baseline


@pytest.mark.parametrize(
    ("field", "malformed", "diagnostic"),
    (
        ("table_comment", False, "table comment is not a string"),
        ("column_comment", 0, "column 'id' comment is not a string"),
        ("layout_item", {"name": "region"}, "partition_columns entries must be strings"),
        ("property_value", True, "table_properties values must be strings"),
    ),
)
def test_non_string_describe_leaf_values_raise_metadata_parse_error(
    field: str,
    malformed: object,
    diagnostic: str,
) -> None:
    # Given an otherwise valid document with one malformed leaf value
    document = json.loads(_doc())
    if field == "table_comment":
        document["comment"] = malformed
    elif field == "column_comment":
        document["columns"][0]["comment"] = malformed
    elif field == "layout_item":
        document["partition_columns"] = [malformed]
    else:
        document["table_properties"] = {"delta.appendOnly": malformed}

    # When parsing the document
    with pytest.raises(MetadataParseError) as exc_info:
        _parse(json.dumps(document))

    # Then the diagnostic identifies the malformed field
    assert diagnostic in str(exc_info.value)
