import pytest
from dataclasses import FrozenInstanceError
from tabula.domain.model.full_name import FullName
from tabula.domain.model.column import Column
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.data_type import DataType

def test_tablespec_stores_identity_and_preserves_column_order():
    full_name = FullName("dev", "sales", "orders")
    columns = (
        Column("id", DataType("integer"), is_nullable=False),
        Column("amount", DataType("decimal", (18, 2)), is_nullable=False),
        Column("note", DataType("string"), is_nullable=True),
    )
    spec = TableSpec(full_name=full_name, columns=columns)

    assert spec.full_name == full_name
    assert spec.columns == columns
    assert [c.name for c in spec.columns] == ["id", "amount", "note"]

def test_tablespec_is_immutable_and_hashable():
    spec = TableSpec(
        full_name=FullName("dev", "finance", "payments"),
        columns=(Column("id", DataType("integer"), False),),
    )
    with pytest.raises(FrozenInstanceError):
        spec.columns = ()  # type: ignore[attr-defined]
    # frozen dataclasses with hashable fields should be hashable
    _ = hash(spec)

def test_tablespec_value_equality_is_by_fields():
    a = TableSpec(
        full_name=FullName("dev", "sales", "orders"),
        columns=(Column("id", DataType("integer"), False),),
    )
    b = TableSpec(
        full_name=FullName("dev", "sales", "orders"),
        columns=(Column("id", DataType("integer"), False),),
    )
    c = TableSpec(
        full_name=FullName("prod", "sales", "orders"),
        columns=(Column("id", DataType("integer"), False),),
    )
    assert a == b
    assert a != c
