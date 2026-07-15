from delta_engine.adapters.databricks.sql.types import data_type_from_json, render_data_type
from delta_engine.domain.model.data_type import (
    Array,
    Binary,
    Boolean,
    Byte,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Map,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
    Variant,
)


def test_sql_type_for_primitive_types() -> None:
    assert render_data_type(Integer()) == "INT"
    assert render_data_type(Long()) == "BIGINT"
    assert render_data_type(Byte()) == "TINYINT"
    assert render_data_type(Short()) == "SMALLINT"
    assert render_data_type(Float()) == "FLOAT"
    assert render_data_type(Double()) == "DOUBLE"
    assert render_data_type(Boolean()) == "BOOLEAN"
    assert render_data_type(String()) == "STRING"
    assert render_data_type(Binary()) == "BINARY"
    assert render_data_type(Date()) == "DATE"
    assert render_data_type(Timestamp()) == "TIMESTAMP"
    assert render_data_type(TimestampNtz()) == "TIMESTAMP_NTZ"
    assert render_data_type(Variant()) == "VARIANT"


def test_sql_type_for_decimal_array_map_recursive() -> None:
    assert render_data_type(Decimal(10, 2)) == "DECIMAL(10,2)"
    assert render_data_type(Array(String())) == "ARRAY<STRING>"
    assert render_data_type(Map(String(), Integer())) == "MAP<STRING,INT>"
    # nested
    nested = Array(Map(String(), Decimal(9, 0)))
    assert render_data_type(nested) == "ARRAY<MAP<STRING,DECIMAL(9,0)>>"


def test_sql_type_for_struct_renders_fields_in_order() -> None:
    struct = Struct((StructField("a", Integer()), StructField("b", Array(String()))))
    assert render_data_type(struct) == "STRUCT<`a`: INT, `b`: ARRAY<STRING>>"


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
