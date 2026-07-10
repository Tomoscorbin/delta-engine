from delta_engine.adapters.databricks.sql.types import sql_type_for_data_type
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
    assert sql_type_for_data_type(Integer()) == "INT"
    assert sql_type_for_data_type(Long()) == "BIGINT"
    assert sql_type_for_data_type(Byte()) == "TINYINT"
    assert sql_type_for_data_type(Short()) == "SMALLINT"
    assert sql_type_for_data_type(Float()) == "FLOAT"
    assert sql_type_for_data_type(Double()) == "DOUBLE"
    assert sql_type_for_data_type(Boolean()) == "BOOLEAN"
    assert sql_type_for_data_type(String()) == "STRING"
    assert sql_type_for_data_type(Binary()) == "BINARY"
    assert sql_type_for_data_type(Date()) == "DATE"
    assert sql_type_for_data_type(Timestamp()) == "TIMESTAMP"
    assert sql_type_for_data_type(TimestampNtz()) == "TIMESTAMP_NTZ"
    assert sql_type_for_data_type(Variant()) == "VARIANT"


def test_sql_type_for_decimal_array_map_recursive() -> None:
    assert sql_type_for_data_type(Decimal(10, 2)) == "DECIMAL(10,2)"
    assert sql_type_for_data_type(Array(String())) == "ARRAY<STRING>"
    assert sql_type_for_data_type(Map(String(), Integer())) == "MAP<STRING,INT>"
    # nested
    nested = Array(Map(String(), Decimal(9, 0)))
    assert sql_type_for_data_type(nested) == "ARRAY<MAP<STRING,DECIMAL(9,0)>>"


def test_sql_type_for_struct_renders_fields_in_order() -> None:
    struct = Struct((StructField("a", Integer()), StructField("b", Array(String()))))
    assert sql_type_for_data_type(struct) == "STRUCT<`a`: INT, `b`: ARRAY<STRING>>"
