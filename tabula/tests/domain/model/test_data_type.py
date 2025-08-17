from tabula.domain.model.data_type import DataType
from tabula.domain.model import types as dt

def test_scalar_builders_and_specification_property():
    assert dt.big_integer().specification == "big_integer"
    assert dt.boolean().specification == "boolean"
    assert dt.string().specification == "string"

def test_parameterised_types_specification():
    assert dt.decimal(18, 2).specification == "decimal(18,2)"
    assert dt.varchar(100).specification == "varchar(100)"

def test_equality_is_field_based_and_str_matches_specification():
    a = DataType("decimal", (18, 2))
    b = DataType("decimal", (18, 2))
    c = DataType("DECIMAL", (18, 2))
    assert a == b
    assert a != c          # case differs
    assert str(a) == a.specification
