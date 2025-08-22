from __future__ import annotations
from dataclasses import FrozenInstanceError
import pytest
from tabula.domain.model.data_type import DataType

# ---------------------------
# Construction & normalization
# ---------------------------

def test_name_is_lowercased_ascii():
    dt = DataType("BIGINT")
    assert dt.name == "bigint"

def test_equality_is_case_insensitive_on_name_and_sensitive_on_params():
    a = DataType("STRING")
    b = DataType("string")
    c = DataType("string", (1,))  # different params
    assert a == b
    assert hash(a) == hash(b)
    assert a != c

# ---------------------------
# Name validation
# ---------------------------

def test_empty_or_blank_name_rejected():
    with pytest.raises(ValueError): DataType("")
    with pytest.raises(ValueError): DataType("   ")

@pytest.mark.parametrize("bad", [" int", "int ", "\tint", "int\n"])
def test_leading_or_trailing_whitespace_rejected(bad):
    with pytest.raises(ValueError): DataType(bad)

@pytest.mark.parametrize("bad", ["var char", "var\tchar", "var\nchar"])
def test_any_internal_whitespace_rejected(bad):
    with pytest.raises(ValueError): DataType(bad)

def test_dot_in_name_rejected():
    with pytest.raises(ValueError): DataType("my.type")

def test_non_string_name_rejected_with_type_error():
    with pytest.raises(TypeError):
        DataType(123)  # type: ignore[arg-type]

def test_non_ascii_name_rejected():
    with pytest.raises(ValueError):
        DataType("İNT")  # non-ASCII dotted I

# ---------------------------
# Parameter validation (shape & types)
# ---------------------------

def test_parameters_must_be_int_or_datatype():
    with pytest.raises(TypeError):
        DataType("array", ("int",))  # string param not allowed

def test_parameters_non_iterable_rejected():
    class Weird: ...
    with pytest.raises(TypeError):
        DataType("array", Weird())  # not iterable

def test_parameters_list_is_coerced_to_tuple_and_immutable():
    lst = [DataType("int")]
    dt = DataType("array", lst)
    assert isinstance(dt.parameters, tuple)
    assert dt.parameters == (DataType("int"),)
    # Mutating the original list does not affect the DataType
    lst.append(DataType("string"))
    assert dt.parameters == (DataType("int"),)

def test_nested_datatype_parameter_value_semantics():
    a = DataType("array", (DataType("INT"),))
    b = DataType("array", (DataType("int"),))
    assert a == b
    assert hash(a) == hash(b)

# ---------------------------
# Decimal semantics
# ---------------------------

def test_decimal_requires_two_ints():
    with pytest.raises(ValueError): DataType("decimal")                    # missing params
    with pytest.raises(ValueError): DataType("decimal", (18,))             # one param
    with pytest.raises(TypeError):  DataType("decimal", (18, "2"))         # wrong type
    with pytest.raises(ValueError): DataType("decimal", (18, 19))          # scale > precision

@pytest.mark.parametrize("precision, scale", [(0, 0), (-1, 0), (10, -1)])
def test_decimal_invalid_precision_or_scale(precision, scale):
    with pytest.raises(ValueError):
        DataType("decimal", (precision, scale))

def test_decimal_valid_edge_cases():
    DataType("decimal", (1, 0))   # min precision, zero scale
    DataType("decimal", (10, 10)) # scale == precision

# ---------------------------
# Immutability (frozen dataclass)
# ---------------------------

def test_frozen_dataclass_prevents_field_reassignment():
    dt = DataType("int")
    with pytest.raises(FrozenInstanceError):
        dt.name = "string"  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError):
        dt.parameters = ()  # type: ignore[attr-defined]
