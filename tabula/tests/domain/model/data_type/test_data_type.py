from __future__ import annotations
from dataclasses import FrozenInstanceError
import pytest
from tabula.domain.model.data_type.data_type import (
    DataType,
    _coerce_params,
    _validate_by_type,
    register_type,
)


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
    with pytest.raises(ValueError):
        DataType("decimal")  # missing params
    with pytest.raises(ValueError):
        DataType("decimal", (18,))  # one param
    with pytest.raises(ValueError):
        DataType("decimal", (18, "2"))  # wrong type
    with pytest.raises(ValueError):
        DataType("decimal", (18, 19))  # scale > precision


@pytest.mark.parametrize("precision, scale", [(0, 0), (-1, 0), (10, -1)])
def test_decimal_invalid_precision_or_scale(precision, scale):
    with pytest.raises(ValueError):
        DataType("decimal", (precision, scale))


def test_decimal_valid_edge_cases():
    DataType("decimal", (1, 0))  # min precision, zero scale
    DataType("decimal", (10, 10))  # scale == precision


# ---------------------------
# Immutability (frozen dataclass)
# ---------------------------


def test_frozen_dataclass_prevents_field_reassignment():
    dt = DataType("int")
    with pytest.raises(FrozenInstanceError):
        dt.name = "string"  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError):
        dt.parameters = ()  # type: ignore[attr-defined]


def test__coerce_params_tuple_passthrough():
    raw = (1, DataType("int"))
    out = _coerce_params(raw)
    assert out is raw


def test__coerce_params_iterable_to_tuple():
    raw = [1, DataType("int")]
    out = _coerce_params(raw)
    assert isinstance(out, tuple)
    assert out == (1, DataType("int"))


@pytest.mark.parametrize("bad", [None, 1, 1.2, object()])
def test__coerce_params_non_iterable_raises(bad):
    with pytest.raises(TypeError) as exc:
        _coerce_params(bad)  # type: ignore[arg-type]
    assert "iterable" in str(exc.value)


def test__validate_by_type_noop_for_unknown():
    _validate_by_type("unknown", ())  # no raise


def test_decimal_validator_happy():
    _validate_by_type("decimal", (18, 2))


@pytest.mark.parametrize("params", [(18,), (18, 2, 3), ("18", "2"), (18, -1), (2, 10)])
def test_decimal_validator_errors(params):
    with pytest.raises(Exception):
        _validate_by_type("decimal", params)  # type: ignore[arg-type]


def test_array_validator_shape_and_type():
    _validate_by_type("array", (DataType("int"),))  # ok
    with pytest.raises(ValueError):
        _validate_by_type("array", ())
    with pytest.raises(ValueError):
        _validate_by_type("array", (DataType("int"), DataType("int")))
    with pytest.raises(TypeError):
        _validate_by_type("array", (123,))  # type: ignore[arg-type]


def test_map_validator_shape_and_type():
    _validate_by_type("map", (DataType("string"), DataType("int")))  # ok
    with pytest.raises(ValueError):
        _validate_by_type("map", (DataType("string"),))
    with pytest.raises(TypeError):
        _validate_by_type("map", (DataType("string"), 1))  # type: ignore[arg-type]


def test_register_type_custom_rule():
    calls = {}

    @register_type("custom")
    def _validate(params):
        calls["seen"] = params
        if params != ("ok",):
            raise ValueError("bad")

    _validate_by_type("custom", ("ok",))
    assert calls["seen"] == ("ok",)
    with pytest.raises(ValueError):
        _validate_by_type("custom", ("nope",))
