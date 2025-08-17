import pytest
from dataclasses import FrozenInstanceError
from tabula.domain.model.full_name import FullName
from tabula.domain.model.column import Column
from tabula.domain.model.table_state import TableState
from tabula.domain.model.data_type import DataType

def test_tablestate_stores_identity_and_preserves_column_order():
    full_name = FullName("dev", "sales", "orders")
    columns = (
        Column("id", DataType("integer"), is_nullable=False),
        Column("amount", DataType("decimal", (18, 2)), is_nullable=False),
        Column("note", DataType("string"), is_nullable=True),
    )
    spec = TableState(full_name=full_name, columns=columns)

    assert spec.full_name == full_name
    assert spec.columns == columns
    assert [c.name for c in spec.columns] == ["id", "amount", "note"]

def test_tablestate_is_immutable_and_hashable():
    spec = TableState(
        full_name=FullName("dev", "finance", "payments"),
        columns=(Column("id", DataType("integer"), False),),
    )
    with pytest.raises(FrozenInstanceError):
        spec.columns = ()  # type: ignore[attr-defined]
    # frozen dataclasses with hashable fields should be hashable
    _ = hash(spec)

def test_TableState_value_equality_is_by_fields():
    a = TableState(
        full_name=FullName("dev", "sales", "orders"),
        columns=(Column("id", DataType("integer"), False),),
    )
    b = TableState(
        full_name=FullName("dev", "sales", "orders"),
        columns=(Column("id", DataType("integer"), False),),
    )
    c = TableState(
        full_name=FullName("prod", "sales", "orders"),
        columns=(Column("id", DataType("integer"), False),),
    )
    assert a == b
    assert a != c
