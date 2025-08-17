import pytest
from tabula.domain.model.data_type import DataType

def test_name_normalized_and_spec_no_params():
    t = DataType("BIGINT")
    assert str(t) == "bigint"

def test_decimal_validation():
    with pytest.raises(ValueError): DataType("decimal", (18,))    # wrong arity
    with pytest.raises(ValueError): DataType("decimal", ("a","b"))  # not ints
    with pytest.raises(ValueError): DataType("decimal", (10, 20))   # scale > precision
    assert str(DataType("decimal", (18, 2))) == "decimal(18,2)"

def test_name_is_normalized_to_lowercase():
    t = DataType("BIGINT")
    assert t.name == "bigint"
    assert str(t) == "bigint"

def test_decimal_boundary_values():
    # legal: scale == 0, scale == precision
    assert str(DataType("decimal", (18, 0))) == "decimal(18,0)"
    assert str(DataType("decimal", (9, 9))) == "decimal(9,9)"
    # illegal: zero/negative precision, negative scale, scale > precision
    with pytest.raises(ValueError): DataType("decimal", (0, 0))
    with pytest.raises(ValueError): DataType("decimal", (-1, 0))
    with pytest.raises(ValueError): DataType("decimal", (10, -1))
    with pytest.raises(ValueError): DataType("decimal", (10, 11))

def test_nested_types_render_in_specification():
    # even if you do not fully validate array/map/struct yet, the spec should render.
    t_array_int = DataType("array", (DataType("int"),))
    assert str(t_array_int) == "array(int)"
    t_array_decimal = DataType("array", (DataType("decimal", (18, 2)),))
    assert str(t_array_decimal) == "array(decimal(18,2))"

def test_parameters_must_be_correct_shape_for_decimal():
    with pytest.raises(ValueError): DataType("decimal", (18,))
    with pytest.raises(ValueError): DataType("decimal", ("18", "2"))  # wrong types