from delta_engine.adapters.databricks.sql.types import render_data_type
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
