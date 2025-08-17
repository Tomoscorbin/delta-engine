import pytest
from dataclasses import FrozenInstanceError
from tabula.domain.model.full_name import FullName

def test_qualified_name_and_str():
    full_name = FullName("dev", "sales", "orders")
    assert full_name.qualified_name() == "dev.sales.orders"
    assert str(full_name) == "dev.sales.orders"

def test_immutability():
    full_name = FullName("dev", "sales", "orders")
    with pytest.raises(FrozenInstanceError):
        full_name.name = "new"

def test_value_equality_and_hash():
    a = FullName("dev", "sales", "orders")
    b = FullName("dev", "sales", "orders")
    c = FullName("prod", "sales", "orders")
    assert a == b and hash(a) == hash(b)
    assert a != c
    assert len({a, b}) == 1
