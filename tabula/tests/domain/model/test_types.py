import pytest
from tabula.domain.model.types import (
    bigint, integer, smallint, boolean, string, date, timestamp,
    double, float32, float64, floating_point, decimal
)
from tabula.domain.model.data_type import DataType


def test_scalar_factories_return_expected_values():
    cases = [
        (bigint,    "bigint"),
        (integer,   "int"),
        (smallint,  "smallint"),
        (boolean,   "boolean"),
        (string,    "string"),
        (date,      "date"),
        (timestamp, "timestamp"),
        (double,    "double"),
        (float32,   "float"),
        (float64,   "double"),
    ]
    for factory, expected_name in cases:
        dt = factory()
        assert isinstance(dt, DataType)
        assert dt == DataType(expected_name)
        assert getattr(dt, "parameters", ()) == ()


def test_factories_return_equal_values_on_multiple_calls():
    # Value equality is the contract
    assert integer() == integer()
    assert string() == string()
    # Hash is stable for equal values
    assert hash(double()) == hash(double())


def test_decimal_factory_value_semantics():
    dt = decimal(18, 2)
    assert isinstance(dt, DataType)
    assert dt == DataType("decimal", (18, 2))


def test_decimal_factory_invalid_inputs_raise():
    with pytest.raises(ValueError):
        decimal(0, 0)          # precision must be > 0
    with pytest.raises(ValueError):
        decimal(10, 11)        # scale > precision not allowed
    with pytest.raises(ValueError):
        decimal(10, -1)        # negative scale not allowed


def test_floating_point_accepts_only_32_or_64():
    # If singletons are part of the API, keep identity checks:
    assert floating_point(32) is float32()
    assert floating_point(64) is float64()
    with pytest.raises(ValueError):
        floating_point(16)


def test_float_aliases_are_consistent():
    # Ensure synonyms map to the same value (or same singleton if that’s intentional)
    assert float64() == double()