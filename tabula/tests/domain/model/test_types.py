import pytest
from tabula.domain.model.types import (
    bigint, integer, smallint, boolean, string, date, timestamp,
    double, float32, float64, floating_point, decimal
)
from tabula.domain.model.data_type import DataType


def test_scalar_factories_return_expected_names_and_types():
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
    for fn, expected in cases:
        dt = fn()
        assert isinstance(dt, DataType)
        assert str(dt) == expected


def test_factories_return_equal_values_on_multiple_calls():
    # We don’t assert identity; value equality is the contract.
    assert integer() == integer()
    assert string() == string()
    assert hash(double()) == hash(double())


def test_decimal_factory_round_trips_to_string():
    dt = decimal(18, 2)
    assert isinstance(dt, DataType)
    assert str(dt) == "decimal(18,2)"


def test_decimal_factory_invalid_inputs_raise():
    with pytest.raises(ValueError):
        decimal(0, 0)       # precision must be > 0, enforced in DataType
    with pytest.raises(ValueError):
        decimal(10, 11)     # scale > precision


def test_floating_point_accepts_only_32_or_64():
    assert str(floating_point(32)) == "float"
    assert str(floating_point(64)) == "double"
    with pytest.raises(ValueError):
        floating_point(16)  # runtime guard
