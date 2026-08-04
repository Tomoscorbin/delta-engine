from hypothesis import given
import pytest

from delta_engine.adapters.databricks.sql.types import data_type_from_json, render_data_type
from delta_engine.domain.model.data_type import (
    Boolean,
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
    assert render_data_type(case.data_type) == case.sql


def test_primitive_aliases():
    assert data_type_from_json({"name": "int"}) == Integer()
    assert data_type_from_json({"name": "integer"}) == Integer()
    assert data_type_from_json({"name": "bigint"}) == Long()
    assert data_type_from_json({"name": "double"}) == Double()
    assert data_type_from_json({"name": "boolean"}) == Boolean()


def test_string_like_types_accept_the_default_collation() -> None:
    assert data_type_from_json({"name": "string"}) == String()
    assert data_type_from_json({"name": "string", "collation": "UTF8_BINARY"}) == String()
    assert data_type_from_json({"name": "varchar", "length": 20}) == String()


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
    assert data_type_from_json(document) is None


def test_timestamp_ltz_aliases_to_timestamp():
    assert data_type_from_json({"name": "timestamp"}) == Timestamp()
    assert data_type_from_json({"name": "timestamp_ltz"}) == Timestamp()
    assert data_type_from_json({"name": "timestamp_ntz"}) == TimestampNtz()


def test_decimal_without_concrete_precision_and_scale_is_unmappable():
    # Unity Catalog records concrete precision and scale for every decimal
    # column. Rather than invent DECIMAL(10,0) — false type drift against
    # whatever the column really is — an absent or non-numeric field is
    # unmappable and fails the read like any other unreadable type.
    assert data_type_from_json({"name": "decimal"}) is None
    assert data_type_from_json({"name": "decimal", "precision": 12}) is None
    assert data_type_from_json({"name": "decimal", "scale": 4}) is None
    assert data_type_from_json({"name": "decimal", "precision": "abc", "scale": 2}) is None
    assert data_type_from_json({"name": "decimal", "precision": [10], "scale": 2}) is None


def test_unmappable_returns_none():
    assert data_type_from_json({"name": "interval"}) is None
    assert (
        data_type_from_json(
            {
                "name": "struct",
                "fields": [
                    {"name": "a", "type": {"name": "int"}, "nullable": True},
                    {"name": "A", "type": {"name": "int"}, "nullable": True},
                ],
            }
        )
        is None
    )  # duplicate field identifier
    assert (
        data_type_from_json(
            {
                "name": "struct",
                "fields": [{"name": "id", "type": {"name": "int"}, "nullable": 1}],
            }
        )
        is None
    )
    assert (
        data_type_from_json({"name": "struct", "fields": [{"name": "id", "type": {"name": "int"}}]})
        is None
    )
    assert data_type_from_json({"not": "a type"}) is None
    assert data_type_from_json("string") is None


def test_blank_struct_field_name_returns_none():
    assert (
        data_type_from_json(
            {
                "name": "struct",
                "fields": [{"name": "  ", "type": {"name": "int"}, "nullable": True}],
            }
        )
        is None
    )


def test_decimal_over_delta_limit_returns_none():
    assert data_type_from_json({"name": "decimal", "precision": 40, "scale": 2}) is None


def test_pathologically_deep_nesting_returns_none():
    payload = {"name": "int"}
    for _ in range(6000):
        payload = {"name": "array", "element_type": payload}
    assert data_type_from_json(payload) is None


@given(TYPE_CASES)
def test_canonical_json_type_documents_map_to_the_generated_domain_type(case: TypeCase) -> None:
    assert data_type_from_json(case.document) == case.data_type


@pytest.mark.parametrize(
    ("field", "malformed"),
    (
        ("precision", True),
        ("precision", 10.5),
        ("precision", "10"),
        ("scale", False),
        ("scale", 2.5),
        ("scale", "2"),
    ),
)
def test_decimal_rejects_non_integer_json_fields(field: str, malformed: object) -> None:
    document: dict[str, object] = {"name": "decimal", "precision": 10, "scale": 2}
    document[field] = malformed

    assert data_type_from_json(document) is None
