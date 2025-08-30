from delta_engine.domain.model.data_type import (
    Boolean,
    Date,
    Double,
    Float,
    Integer,
    Long,
    String,
    Timestamp,
)
from delta_engine.schema import types as schema_types


def test_schema_type_singletons_present_and_types() -> None:
    assert isinstance(schema_types.INTEGER, Integer)
    assert isinstance(schema_types.LONG, Long)
    assert isinstance(schema_types.FLOAT, Float)
    assert isinstance(schema_types.DOUBLE, Double)
    assert isinstance(schema_types.BOOLEAN, Boolean)
    assert isinstance(schema_types.STRING, String)
    assert isinstance(schema_types.DATE, Date)
    assert isinstance(schema_types.TIMESTAMP, Timestamp)
