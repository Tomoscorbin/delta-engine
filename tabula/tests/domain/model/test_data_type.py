import pytest

from tabula.domain.model.data_type import DataType


# ---------------------------
# Construction & normalization
# ---------------------------

def test_name_is_casefolded_and_no_params_str():
    dt = DataType("BIGINT")
    assert dt.name == "bigint"
    assert str(dt) == "bigint"


def test_str_with_params_and_nesting_parentheses_style():
    arr_int = DataType("array", (DataType("int"),))
    assert str(arr_int) == "array(int)"

    map_str_int = DataType("map", (DataType("string"), DataType("int")))
    assert str(map_str_int) == "map(string,int)"


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
    with pytest.raises(ValueError):
        DataType("")
    with pytest.raises(ValueError):
        DataType("   ")


@pytest.mark.parametrize("bad", [" int", "int ", "\tint", "int\n"])
def test_leading_or_trailing_whitespace_rejected(bad):
    with pytest.raises(ValueError):
        DataType(bad)


@pytest.mark.parametrize("bad", ["var char", "var\tchar", "var\nchar"])
def test_any_internal_whitespace_rejected(bad):
    with pytest.raises(ValueError):
        DataType(bad)


def test_dot_in_name_rejected():
    with pytest.raises(ValueError):
        DataType("my.type")


def test_non_string_name_rejected_with_type_error():
    with pytest.raises(TypeError):
        DataType(123)  # type: ignore[arg-type]


# ---------------------------
# Parameter validation (shape & types)
# ---------------------------

def test_parameters_must_be_int_or_datatype():
    with pytest.raises(TypeError):
        DataType("array", ("int",))  # string param not allowed


def test_parameters_non_iterable_rejected():
    class Weird:
        pass
    with pytest.raises(TypeError):
        DataType("array", Weird())  # not tuple/iterable


# ---------------------------
# Decimal semantics
# ---------------------------

def test_decimal_requires_two_ints():
    with pytest.raises(ValueError):
        DataType("decimal")                    # missing params
    with pytest.raises(ValueError):
        DataType("decimal", (18,))             # one param
    with pytest.raises(TypeError):
        DataType("decimal", (18, "2"))         # wrong type -> TypeError (type validation)
    with pytest.raises(ValueError):
        DataType("decimal", (18, 19))          # scale > precision



@pytest.mark.parametrize("precision, scale", [(18, 2), (1, 0), (38, 38)])
def test_decimal_valid_examples(precision, scale):
    dt = DataType("decimal", (precision, scale))
    assert str(dt) == f"decimal({precision},{scale})"


@pytest.mark.parametrize("precision, scale", [(0, 0), (-1, 0), (10, -1)])
def test_decimal_invalid_precision_or_scale(precision, scale):
    with pytest.raises(ValueError):
        DataType("decimal", (precision, scale))
