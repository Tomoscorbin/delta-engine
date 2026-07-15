# tests/adapters/databricks/sql/test_describe_json.py
from delta_engine.adapters.databricks.sql.describe_json import data_type_from_json
from delta_engine.domain.model import (
    Array,
    Boolean,
    Decimal,
    Double,
    Integer,
    Long,
    Map,
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
