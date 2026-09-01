from hypothesis import given
import pytest

from delta_engine.adapters.databricks.sql.types import data_type_from_json, render_data_type
from delta_engine.domain.model.data_type import (
    Boolean,
    DataType,
    Double,
    Integer,
    Long,
    String,
    Timestamp,
    TimestampNtz,
)
from tests.adapters.databricks.sql.strategies import TYPE_CASES, TypeCase


@given(TYPE_CASES)
def test_supported_types_render_to_their_canonical_sql(case: TypeCase) -> None:
    # Then every modeled type renders to its canonical Databricks SQL spelling
    assert render_data_type(case.data_type) == case.sql


@given(TYPE_CASES)
def test_canonical_json_type_documents_map_to_the_generated_domain_type(case: TypeCase) -> None:
    # Then every canonical catalog document maps back to its domain type
    assert data_type_from_json(case.document) == case.data_type


@pytest.mark.parametrize(
    ("document", "expected"),
    (
        ({"name": "int"}, Integer()),
        ({"name": "integer"}, Integer()),
        ({"name": "bigint"}, Long()),
        ({"name": "double"}, Double()),
        ({"name": "boolean"}, Boolean()),
        ({"name": "string"}, String()),
        ({"name": "string", "collation": "UTF8_BINARY"}, String()),
        ({"name": "varchar", "length": 20}, String()),
        ({"name": "timestamp"}, Timestamp()),
        ({"name": "timestamp_ltz"}, Timestamp()),
        ({"name": "timestamp_ntz"}, TimestampNtz()),
    ),
)
def test_catalog_type_spellings_map_to_their_domain_type(
    document: dict[str, object], expected: DataType
) -> None:
    # Then each catalog spelling — aliases, the default collation, and
    # varchar included — maps to its single domain type
    assert data_type_from_json(document) == expected


@pytest.mark.parametrize(
    "document",
    (
        {"name": "string", "collation": "UTF8_LCASE"},
        {"name": "varchar", "length": 20, "collation": "UTF8_LCASE"},
        {"name": "string", "collation": None},
        {
            "name": "array",
            "element_type": {"name": "string", "collation": "UTF8_LCASE"},
        },
    ),
)
def test_string_like_types_reject_unsupported_or_malformed_collations(
    document: dict[str, object],
) -> None:
    # Then a non-default collation is unmappable, nested occurrences included
    assert data_type_from_json(document) is None


# Unity Catalog records concrete precision and scale for every decimal
# column. Rather than invent DECIMAL(10,0) — false type drift against
# whatever the column really is — an absent, non-integer, or over-limit
# field is unmappable and fails the read like any other unreadable type.
@pytest.mark.parametrize(
    "document",
    (
        pytest.param({"name": "decimal"}, id="both-absent"),
        pytest.param({"name": "decimal", "precision": 12}, id="scale-absent"),
        pytest.param({"name": "decimal", "scale": 4}, id="precision-absent"),
        pytest.param({"name": "decimal", "precision": "abc", "scale": 2}, id="precision-string"),
        pytest.param({"name": "decimal", "precision": [10], "scale": 2}, id="precision-list"),
        pytest.param({"name": "decimal", "precision": True, "scale": 2}, id="precision-bool"),
        pytest.param({"name": "decimal", "precision": 10.5, "scale": 2}, id="precision-float"),
        pytest.param({"name": "decimal", "precision": "10", "scale": 2}, id="precision-numeral"),
        pytest.param({"name": "decimal", "precision": 10, "scale": False}, id="scale-bool"),
        pytest.param({"name": "decimal", "precision": 10, "scale": 2.5}, id="scale-float"),
        pytest.param({"name": "decimal", "precision": 10, "scale": "2"}, id="scale-numeral"),
        pytest.param({"name": "decimal", "precision": 40, "scale": 2}, id="over-delta-limit"),
    ),
)
def test_decimal_without_concrete_in_range_precision_and_scale_is_unmappable(
    document: dict[str, object],
) -> None:
    assert data_type_from_json(document) is None


@pytest.mark.parametrize(
    "document",
    (
        pytest.param({"name": "interval"}, id="unknown-type-name"),
        pytest.param(
            {
                "name": "struct",
                "fields": [
                    {"name": "a", "type": {"name": "int"}, "nullable": True},
                    {"name": "A", "type": {"name": "int"}, "nullable": True},
                ],
            },
            id="duplicate-struct-field-identifier",
        ),
        pytest.param(
            {
                "name": "struct",
                "fields": [{"name": "id", "type": {"name": "int"}, "nullable": 1}],
            },
            id="non-boolean-struct-nullable",
        ),
        pytest.param(
            {"name": "struct", "fields": [{"name": "id", "type": {"name": "int"}}]},
            id="struct-nullable-missing",
        ),
        pytest.param(
            {
                "name": "struct",
                "fields": [{"name": "  ", "type": {"name": "int"}, "nullable": True}],
            },
            id="blank-struct-field-name",
        ),
        pytest.param({"not": "a type"}, id="type-name-missing"),
        pytest.param("string", id="document-not-an-object"),
    ),
)
def test_unmappable_type_documents_map_to_none(document: object) -> None:
    assert data_type_from_json(document) is None


def test_pathologically_deep_nesting_returns_none():
    # Given a document nested past any legitimate catalog schema
    payload = {"name": "int"}
    for _ in range(6000):
        payload = {"name": "array", "element_type": payload}

    # Then the parse gives up rather than exhausting the stack
    assert data_type_from_json(payload) is None
