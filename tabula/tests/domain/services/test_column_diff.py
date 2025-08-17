from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.services.column_diff import (
    diff_columns_for_adds, diff_columns_for_drops, diff_columns
)
from tabula.domain.model.actions import AddColumn, DropColumn

def col(name: str) -> Column:
    return Column(name, DataType("string"))

def test_diff_columns_for_adds_preserves_spec_order():
    desired = (col("a"), col("b"), col("c"))
    observed = (col("a"),)
    adds = diff_columns_for_adds(desired, observed)
    assert tuple(a.column.name for a in adds) == ("b", "c")

def test_diff_columns_for_drops_sorts_names():
    desired = (col("a"),)
    observed = (col("a"), col("Z"), col("b"))
    drops = diff_columns_for_drops(desired, observed)
    assert tuple(d.column_name for d in drops) == ("b", "Z")  # case-insensitive sorted

def test_diff_columns_combines_adds_then_drops():
    desired = (col("a"), col("b"))
    observed = (col("a"), col("old"))
    actions = diff_columns(desired, observed)
    kinds = tuple(type(a).__name__ for a in actions)
    assert kinds == ("AddColumn", "DropColumn")
    assert isinstance(actions[0], AddColumn) and actions[0].column.name == "b"
    assert isinstance(actions[1], DropColumn) and actions[1].column_name == "old"
