import pytest
from tabula.domain.validation.coercions import coerce_params, validate_param_types
from tabula.domain.model.data_type import DataType

def test_coerce_params_tuple_passthrough():
    raw = (1, DataType("int"))
    out = coerce_params(raw)
    assert out is raw

def test_coerce_params_iterable_to_tuple():
    raw = [1, DataType("int")]
    out = coerce_params(raw)
    assert isinstance(out, tuple)
    assert out == (1, DataType("int"))

@pytest.mark.parametrize("bad", [None, 1, 1.2, object()])
def test_coerce_params_non_iterable_raises(bad):
    with pytest.raises(TypeError) as exc:
        coerce_params(bad)  # type: ignore[arg-type]
    assert "iterable" in str(exc.value)

def test_validate_param_types_accepts_int_and_datatype():
    validate_param_types((18, DataType("int")))  # no raise

@pytest.mark.parametrize("bad", [(DataType("int"), "18"), ([],), ({},), (1.2,)])
def test_validate_param_types_rejects_others(bad):
    with pytest.raises(TypeError) as exc:
        validate_param_types(bad)  # type: ignore[arg-type]
    assert "Invalid parameter type" in str(exc.value)
