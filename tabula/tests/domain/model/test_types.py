from tabula.domain.model.types import (
    bigint, smallint, integer, double, float32, float64, floating_point,
    decimal, boolean, string, date, timestamp
)
def test_scalar_builders_spec():
    assert str(bigint()) == "bigint"
    assert str(smallint()) == "smallint"
    assert str(integer()) == "int"
    assert str(double()) == "double"
    assert str(float32()) in {"float", "float"}   # Spark 32-bit is 'float'
    assert str(float64()) == "double"
    assert str(string()) == "string"
    assert str(date()) == "date"
    assert str(timestamp()) == "timestamp"
    assert str(boolean()) == "boolean"


def test_floating_point_defaults_and_variants():
    assert str(floating_point()) == "double"
    assert str(floating_point(64)) == "double"
    assert str(floating_point(32)) in {"float"}  # Spark UC 32-bit is 'float'

def test_float64_aliases_double():
    assert str(float64()) == str(double()) == "double"

def test_decimal_builder_round_trips():
    t = decimal(18, 2)
    assert str(t) == "decimal(18,2)"
