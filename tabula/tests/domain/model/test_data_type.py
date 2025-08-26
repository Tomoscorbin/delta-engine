import pytest

from tabula.domain.model.data_type import (
    Array,
    Boolean,
    Decimal,
    Int32,
    Int64,
    Map,
    String,
)


def test_scalar_types_compare_by_type():
    assert Int32() == Int32()
    assert Int32() != Int64()
    assert Boolean() == Boolean()
    assert String() == String()


def test_decimal_requires_valid_precision_scale():
    # valid
    assert Decimal(precision=10, scale=0) == Decimal(10, 0)
    assert Decimal(precision=10, scale=2) == Decimal(10, 2)
    # invalid: precision <= 0
    with pytest.raises(ValueError):
        Decimal(0, 0)
    # invalid: negative scale
    with pytest.raises(ValueError):
        Decimal(10, -1)
    # invalid: scale > precision
    with pytest.raises(ValueError):
        Decimal(5, 6)


def test_array_and_map_hold_nested_types():
    arr = Array(String())
    mp = Map(String(), Int64())
    assert isinstance(arr.element, String.__class__) or isinstance(arr.element, String)
    assert isinstance(mp.key, String.__class__) or isinstance(mp.key, String)
    assert isinstance(mp.value, Int64.__class__) or isinstance(mp.value, Int64)
