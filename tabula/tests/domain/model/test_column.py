import pytest
from dataclasses import FrozenInstanceError
from tabula.domain.model.column import Column
from tabula.domain.model import types as dt

def test_column_holds_fields_and_specification_string():
    col = Column(name="amount", data_type=dt.decimal(18, 2), is_nullable=False)
    assert col.name == "amount"
    assert col.data_type.specification == "decimal(18,2)"
    assert col.is_nullable is False
    assert col.specification == "amount decimal(18,2) not null"

def test_nullable_column_specification_omits_not_null():
    col = Column(name="notes", data_type=dt.string(), is_nullable=True)
    assert col.specification == "notes string"

def test_column_is_immutable_and_hashable():
    col = Column(name="id", data_type=dt.integer(), is_nullable=False)
    with pytest.raises(FrozenInstanceError):
        col.name = "other"  # type: ignore[attr-defined]
    # hashable VO (frozen + hashable fields)
    assert isinstance(hash(col), int)

def test_value_equality_is_by_fields():
    a = Column("id", dt.integer(), False)
    b = Column("id", dt.integer(), False)
    c = Column("id", dt.big_integer(), False)
    assert a == b
    assert a != c
