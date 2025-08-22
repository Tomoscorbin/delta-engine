import pytest
from tabula.domain.validation.type_rules import validate_by_type, register_type
from tabula.domain.model.data_type import DataType

def test_validate_by_type_noop_for_unknown():
    validate_by_type("unknown", ())  # no raise

def test_decimal_validator_happy():
    validate_by_type("decimal", (18, 2))

@pytest.mark.parametrize("params", [(18,), (18, 2, 3), ("18", "2"), (18, -1), (2, 10)])
def test_decimal_validator_errors(params):
    with pytest.raises(Exception):
        validate_by_type("decimal", params)  # type: ignore[arg-type]

def test_array_validator_shape_and_type():
    validate_by_type("array", (DataType("int"),))  # ok
    with pytest.raises(ValueError):
        validate_by_type("array", ())
    with pytest.raises(ValueError):
        validate_by_type("array", (DataType("int"), DataType("int")))
    with pytest.raises(TypeError):
        validate_by_type("array", (123,))  # type: ignore[arg-type]

def test_map_validator_shape_and_type():
    validate_by_type("map", (DataType("string"), DataType("int")))  # ok
    with pytest.raises(ValueError):
        validate_by_type("map", (DataType("string"),))
    with pytest.raises(TypeError):
        validate_by_type("map", (DataType("string"), 1))  # type: ignore[arg-type]

def test_register_type_custom_rule():
    calls = {}
    @register_type("custom")
    def _validate(params):
        calls["seen"] = params
        if params != ("ok",):
            raise ValueError("bad")

    validate_by_type("custom", ("ok",))
    assert calls["seen"] == ("ok",)
    with pytest.raises(ValueError):
        validate_by_type("custom", ("nope",))
